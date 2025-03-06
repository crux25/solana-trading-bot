import {
  ComputeBudgetProgram,
  Connection,
  Keypair,
  PublicKey,
  TransactionMessage,
  VersionedTransaction,
  SystemProgram,
  TransactionInstruction,
} from '@solana/web3.js';
import {
  createAssociatedTokenAccountIdempotentInstruction,
  createCloseAccountInstruction,
  getAccount,
  getAssociatedTokenAddress,
  RawAccount,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import { Liquidity, LiquidityPoolKeysV4, LIQUIDITY_STATE_LAYOUT_V4, LiquidityStateV4, Percent, Token, TokenAmount, BigNumberish } from '@raydium-io/raydium-sdk';
import { MarketCache, PoolCache, SnipeListCache } from './cache';
import { PoolFilters } from './filters';
import { TransactionExecutor } from './transactions';
import { createPoolKeys, logger, NETWORK, sleep } from './helpers';
import { Mutex } from 'async-mutex';
import BN from 'bn.js';
import { WarpTransactionExecutor } from './transactions/warp-transaction-executor';
import { JitoTransactionExecutor } from './transactions/jito-rpc-transaction-executor';
import { getMint } from '@solana/spl-token';

export interface BotConfig {
  wallet: Keypair;
  checkRenounced: boolean;
  checkFreezable: boolean;
  checkBurned: boolean;
  minPoolSize: TokenAmount;
  maxPoolSize: TokenAmount;
  quoteToken: Token;
  quoteAmount: TokenAmount;
  quoteAta: PublicKey;
  oneTokenAtATime: boolean;
  useSnipeList: boolean;
  autoSell: boolean;
  autoBuyDelay: number;
  autoSellDelay: number;
  maxBuyRetries: number;
  maxSellRetries: number;
  unitLimit: number;
  unitPrice: number;
  takeProfit: number;
  stopLoss: number;
  buySlippage: number;
  sellSlippage: number;
  priceCheckInterval: number;
  priceCheckDuration: number;
  filterCheckInterval: number;
  filterCheckDuration: number;
  consecutiveMatchCount: number;
  partialProfitThreshold: number;
  partialProfitSellPercent: number
}

interface TransactionMetrics {
  timestamp: string;
  type: 'buy' | 'sell';
  mint: string;
  gasUsed: number;
  fee: number;
  success: boolean;
  blockTime?: number;
  slot?: number;
}

interface QuickRugCheck {
  lpConcentration: number;
  liquidityUSD: number;
  lastUpdate: number;
}

export class Bot {
  private readonly poolFilters: PoolFilters;
  private readonly snipeListCache?: SnipeListCache;
  private readonly mutex: Mutex;
  private readonly cache = new Map<string, { data: any; timestamp: number }>();
  private sellExecutionCount = 0;
  public readonly isWarp: boolean = false;
  public readonly isJito: boolean = false;
  private readonly QUICK_CHECK_INTERVAL = 3000; // 3 seconds
  private readonly CRITICAL_THRESHOLDS = {
    LP_REMOVAL_PERCENT: 20,     // 20% sudden LP removal
    MIN_LIQUIDITY_USD: 5000,    // $5000 minimum liquidity
    MAX_LP_CONCENTRATION: 90,   // 90% max LP concentration
  };
  private activeMonitors = new Map<string, {
    interval: NodeJS.Timer;
    lastCheck: QuickRugCheck;
  }>();
  private readonly MIN_VALID_PRICE = 0.000000001; // $0.000000001
  private readonly MONITOR_INTERVAL = 3000; // 3 seconds

  constructor(
    private readonly connection: Connection,
    private readonly marketStorage: MarketCache,
    private readonly poolStorage: PoolCache,
    private readonly txExecutor: TransactionExecutor,
    readonly config: BotConfig,
  ) {
    this.isWarp = txExecutor instanceof WarpTransactionExecutor;
    this.isJito = txExecutor instanceof JitoTransactionExecutor;
    this.mutex = new Mutex();
    this.poolFilters = new PoolFilters(connection, {
      quoteToken: this.config.quoteToken,
      minPoolSize: this.config.minPoolSize,
      maxPoolSize: this.config.maxPoolSize,
    });
    if (this.config.useSnipeList) {
      this.snipeListCache = new SnipeListCache();
      this.snipeListCache.init();
    }
  }

  async validate() {
    try {
      await getAccount(this.connection, this.config.quoteAta, this.connection.commitment);
    } catch (error) {
      logger.error(
        `${this.config.quoteToken.symbol} token account not found in wallet: ${this.config.wallet.publicKey.toString()}`,
      );
      return false;
    }
    return true;
  }

  public async buy(accountId: PublicKey, poolState: LiquidityStateV4) {
    logger.trace({ mint: poolState.baseMint }, `Processing new pool...`);
    if (this.config.useSnipeList && !this.snipeListCache?.isInList(poolState.baseMint.toString())) {
      logger.debug({ mint: poolState.baseMint.toString() }, `Skipping buy because token is not in a snipe list`);
      return;
    }
    if (this.config.autoBuyDelay > 0) {
      logger.debug({ mint: poolState.baseMint }, `Waiting for ${this.config.autoBuyDelay} ms before buy`);
      await sleep(this.config.autoBuyDelay);
    }
    if (this.config.oneTokenAtATime) {
      if (this.mutex.isLocked() || this.sellExecutionCount > 0) {
        logger.debug({ mint: poolState.baseMint.toString() }, `Skipping buy because one token at a time is turned on and token is already being processed`);
        return;
      }
      await this.mutex.acquire();
    }
    try {
      const [market, mintAta] = await Promise.all([
        this.marketStorage.get(poolState.marketId.toString()),
        getAssociatedTokenAddress(poolState.baseMint, this.config.wallet.publicKey),
      ]);
      const poolKeys: LiquidityPoolKeysV4 = createPoolKeys(accountId, poolState, market);
      if (!this.config.useSnipeList) {
        const match = await this.filterMatch(poolKeys);
        if (!match) {
          logger.trace({ mint: poolKeys.baseMint.toString() }, `Skipping buy because pool doesn't match filters`);
          return;
        }
      }

      const rugPullRisk = await this.detectRugPullRisk(poolKeys);
      if (!rugPullRisk.safe) {
        logger.warn({ mint: poolState.baseMint, reason: rugPullRisk.reason }, `Skipping buy due to rug pull risk`);
        return;
      }

      for (let i = 0; i < this.config.maxBuyRetries; i++) {
        try {
          logger.info({ mint: poolState.baseMint.toString() }, `Send buy transaction attempt: ${i + 1}/${this.config.maxBuyRetries}`);
          const tokenOut = new Token(TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals);
          const result = await this.swap(
            poolKeys,
            this.config.quoteAta,
            mintAta,
            this.config.quoteToken,
            tokenOut,
            this.config.quoteAmount,
            this.config.sellSlippage,
            this.config.wallet,
            'buy',
          );
          if (result.confirmed) {
            logger.info({ mint: poolState.baseMint.toString(), signature: result.signature, url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}` }, `Confirmed buy tx`);
            // Use the actual purchased token amount from the swap result if available.
            const purchasedTokenAmount = new TokenAmount(new Token(TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals), this.config.quoteAmount.raw, true);
            // Launch trailing stop-loss monitor (existing method)
            this.trailingStopLossMonitor(purchasedTokenAmount, poolKeys, 3);
            // Launch partial profit-taking monitor with your configured thresholds.
            this.partialProfitTakingMonitor(
              purchasedTokenAmount,
              poolKeys,
              this.config.partialProfitThreshold,
              this.config.partialProfitSellPercent
            );

            // Start quick monitoring
            await this.quickRugCheck(poolKeys, purchasedTokenAmount);

            break;
          }
          logger.info({ mint: poolState.baseMint.toString(), signature: result.signature, error: result.error }, `Error confirming buy tx`);
        } catch (error) {
          logger.debug({ mint: poolState.baseMint.toString(), error }, `Error confirming buy transaction`);
        }
      }
    } catch (error) {
      logger.error({ mint: poolState.baseMint.toString(), error }, `Failed to buy token`);
    } finally {
      if (this.config.oneTokenAtATime) {
        this.mutex.release();
      }
    }
  }

  public async sell(accountId: PublicKey, rawAccount: RawAccount) {
    const mintAddress = rawAccount.mint.toString();
    
    // Clear monitor if exists
    if (this.activeMonitors.has(mintAddress)) {
      clearInterval(this.activeMonitors.get(mintAddress)!.interval);
      this.activeMonitors.delete(mintAddress);
    }

    if (this.config.oneTokenAtATime) {
      this.sellExecutionCount++;
    }
    try {
      logger.trace({ mint: rawAccount.mint }, `Processing new token...`);
      const poolData = await this.poolStorage.get(rawAccount.mint.toString());
      if (!poolData) {
        logger.trace({ mint: rawAccount.mint.toString() }, `Token pool data is not found, can't sell`);
        return;
      }
      const tokenIn = new Token(TOKEN_PROGRAM_ID, poolData.state.baseMint, poolData.state.baseDecimal.toNumber());
      const tokenAmountIn = new TokenAmount(tokenIn, rawAccount.amount, true);
      if (tokenAmountIn.isZero()) {
        logger.info({ mint: rawAccount.mint.toString() }, `Empty balance, can't sell`);
        return;
      }
      if (this.config.autoSellDelay > 0) {
        logger.debug({ mint: rawAccount.mint }, `Waiting for ${this.config.autoSellDelay} ms before sell`);
        await sleep(this.config.autoSellDelay);
      }
      const market = await this.marketStorage.get(poolData.state.marketId.toString());
      const poolKeys: LiquidityPoolKeysV4 = createPoolKeys(new PublicKey(poolData.id), poolData.state, market);
      await this.priceMatch(tokenAmountIn, poolKeys);
      for (let i = 0; i < this.config.maxSellRetries; i++) {
        try {
          logger.info({ mint: rawAccount.mint }, `Send sell transaction attempt: ${i + 1}/${this.config.maxSellRetries}`);
          const result = await this.swap(
            poolKeys,
            accountId,
            this.config.quoteAta,
            tokenIn,
            this.config.quoteToken,
            tokenAmountIn,
            this.config.sellSlippage,
            this.config.wallet,
            'sell',
          );
          if (result.confirmed) {
            logger.info({ dex: `https://dexscreener.com/solana/${rawAccount.mint.toString()}?maker=${this.config.wallet.publicKey}`, mint: rawAccount.mint.toString(), signature: result.signature, url: `https://solscan.io/tx/${result.signature}?cluster=${NETWORK}` }, `Confirmed sell tx`);
            break;
          }
          logger.info({ mint: rawAccount.mint.toString(), signature: result.signature, error: result.error }, `Error confirming sell tx`);
        } catch (error) {
          logger.debug({ mint: rawAccount.mint.toString(), error }, `Error confirming sell transaction`);
        }
      }
    } catch (error) {
      logger.error({ mint: rawAccount.mint.toString(), error }, `Failed to sell token`);
    } finally {
      if (this.config.oneTokenAtATime) {
        this.sellExecutionCount--;
      }
    }
  }

  private async swap(
    poolKeys: LiquidityPoolKeysV4,
    ataIn: PublicKey,
    ataOut: PublicKey,
    tokenIn: Token,
    tokenOut: Token,
    amountIn: TokenAmount,
    slippage: number,
    wallet: Keypair,
    direction: 'buy' | 'sell',
  ) {
    const slippagePercent = new Percent(slippage, 100);
    const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });
    const computedAmountOut = Liquidity.computeAmountOut({
      poolKeys,
      poolInfo,
      amountIn,
      currencyOut: tokenOut,
      slippage: slippagePercent,
    });
    const latestBlockhash = await this.connection.getLatestBlockhash();
    const { innerTransaction } = Liquidity.makeSwapFixedInInstruction(
      {
        poolKeys: poolKeys,
        userKeys: {
          tokenAccountIn: ataIn,
          tokenAccountOut: ataOut,
          owner: wallet.publicKey,
        },
        amountIn: amountIn.raw,
        minAmountOut: computedAmountOut.minAmountOut.raw,
      },
      poolKeys.version,
    );
    const messageV0 = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: [
        ...(this.isWarp || this.isJito
          ? []
          : [
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: this.config.unitPrice }),
            ComputeBudgetProgram.setComputeUnitLimit({ units: this.config.unitLimit }),
          ]),
        ...(direction === 'buy'
          ? [
            createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, ataOut, wallet.publicKey, tokenOut.mint),
          ]
          : []),
        ...innerTransaction.instructions,
        ...(direction === 'sell' ? [createCloseAccountInstruction(ataIn, wallet.publicKey, wallet.publicKey)] : []),
      ],
    }).compileToV0Message();
    const transaction = new VersionedTransaction(messageV0);
    transaction.sign([wallet, ...innerTransaction.signers]);
    return this.txExecutor.executeAndConfirm(transaction, wallet, latestBlockhash);
  }

  private async filterMatch(poolKeys: LiquidityPoolKeysV4) {
    if (this.config.filterCheckInterval === 0 || this.config.filterCheckDuration === 0) {
      return true;
    }
    const timesToCheck = this.config.filterCheckDuration / this.config.filterCheckInterval;
    let timesChecked = 0;
    let matchCount = 0;
    do {
      try {
        const shouldBuy = await this.poolFilters.execute(poolKeys);
        if (shouldBuy) {
          matchCount++;
          if (this.config.consecutiveMatchCount <= matchCount) {
            logger.debug({ mint: poolKeys.baseMint.toString() }, `Filter match ${matchCount}/${this.config.consecutiveMatchCount}`);
            return true;
          }
        } else {
          matchCount = 0;
        }
        await sleep(this.config.filterCheckInterval);
      } finally {
        timesChecked++;
      }
    } while (timesChecked < timesToCheck);
    return false;
  }

  private async priceMatch(amountIn: TokenAmount, poolKeys: LiquidityPoolKeysV4) {
    if (this.config.priceCheckDuration === 0 || this.config.priceCheckInterval === 0) {
      return;
    }

    const timesToCheck = this.config.priceCheckDuration / this.config.priceCheckInterval;
    const profitFraction = this.config.quoteAmount.mul(this.config.takeProfit).numerator.div(new BN(100));
    const profitAmount = new TokenAmount(this.config.quoteToken, profitFraction, true);
    const takeProfit = this.config.quoteAmount.add(profitAmount);

    const lossFraction = this.config.quoteAmount.mul(this.config.stopLoss).numerator.div(new BN(100));
    const lossAmount = new TokenAmount(this.config.quoteToken, lossFraction, true);
    const stopLoss = this.config.quoteAmount.subtract(lossAmount);
    const slippage = new Percent(this.config.sellSlippage, 100);

    let timesChecked = 0;
    let maxObservedPrice: TokenAmount | null = null;
    const emergencyDropThreshold = 0.10; // 10% drop triggers emergency sell

    do {
      try {
        const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });
        const computed = Liquidity.computeAmountOut({
          poolKeys,
          poolInfo,
          amountIn,
          currencyOut: this.config.quoteToken,
          slippage,
        });
        // Force cast computed.amountOut as TokenAmount
        const amountOut = computed.amountOut as TokenAmount;

        if (!maxObservedPrice || amountOut.gt(maxObservedPrice)) {
          maxObservedPrice = amountOut;
        }

        // Calculate emergency threshold using BN arithmetic and create a new TokenAmount
        const thresholdRaw = maxObservedPrice!.raw
          .mul(new BN(100 - emergencyDropThreshold * 100))
          .div(new BN(100));
        const thresholdAmount = new TokenAmount(this.config.quoteToken, thresholdRaw, true);

        if (amountOut.lt(thresholdAmount)) {
          logger.warn({ mint: poolKeys.baseMint.toString() }, `Emergency sell triggered: Price dropped >${emergencyDropThreshold * 100}% from peak.`);
          break;
        }

        logger.debug(
          { mint: poolKeys.baseMint.toString() },
          `Take profit: ${takeProfit.toFixed()} | Stop loss: ${stopLoss.toFixed()} | Current: ${amountOut.toFixed()}`
        );
  

        if (amountOut.lt(stopLoss)) break;
        if (amountOut.gt(takeProfit)) break;
        await sleep(this.config.priceCheckInterval);
      } catch (e) {
        logger.trace({ mint: poolKeys.baseMint.toString(), e }, `Failed to check token price`);
      } finally {
        timesChecked++;
      }
    } while (timesChecked < timesToCheck);
  }

  // Adaptive slippage: uses market volatility (placeholder logic)
  private async getMarketVolatility(mint: PublicKey): Promise<number> {
    return Math.random() * 0.05;
  }

  private calculateAdaptiveSlippage(volatility: number): number {
    const baseSlippage = this.config.buySlippage;
    return Math.min(baseSlippage + volatility * 0.01, baseSlippage * 2);
  }

  private async monitorLiquidity(poolAddress: PublicKey): Promise<boolean> {
    try {
      const poolAccountInfo = await this.connection.getAccountInfo(poolAddress);
      if (!poolAccountInfo) {
        logger.warn(`Pool account info not found for ${poolAddress.toBase58()}`);
        return false;
      }
      const poolState = LIQUIDITY_STATE_LAYOUT_V4.decode(poolAccountInfo.data);
      // Using 'depth' as the liquidity metric
      const currentLiquidity = new TokenAmount(this.config.quoteToken, poolState.depth, true);
      if (currentLiquidity.lt(this.config.minPoolSize)) {
        logger.warn(
          `Current liquidity ${currentLiquidity.toFixed()} is below minPoolSize ${this.config.minPoolSize.toFixed()}`
        );
        return false;
      }
      return true;
    } catch (error) {
      logger.error(`Error monitoring liquidity: ${error}`);
      return false;
    }
  }

  private async trackDeveloperWallets(tokenMint: PublicKey): Promise<boolean> {
    try {
      const mintAccount = await getMint(this.connection, tokenMint);
      if (!mintAccount.mintAuthority) return true; // Token is renounced; safe to trade.
      const devWallet = mintAccount.mintAuthority;
      const signatures = await this.connection.getConfirmedSignaturesForAddress2(devWallet, { limit: 10 });
      for (const sigInfo of signatures) {
        const parsedTx = await this.connection.getParsedTransaction(sigInfo.signature);
        if (parsedTx?.transaction?.message?.accountKeys) {
          for (const account of parsedTx.transaction.message.accountKeys) {
            // Use pubkey property from ParsedMessageAccount
            if ((account as any).pubkey && ((account as any).pubkey as PublicKey).toBase58() === devWallet.toBase58()) {
              logger.warn(`Developer wallet ${devWallet.toBase58()} activity detected. Risk may be present.`);
              return false;
            }
          }
        }
      }
      return true;
    } catch (error) {
      logger.error(`Error tracking developer wallet: ${error}`);
      return false;
    }
  }

  private async checkPostTradeSafety(tokenMint: PublicKey, poolId: PublicKey): Promise<boolean> {
    const liquiditySafe = await this.monitorLiquidity(poolId);
    const devSafe = await this.trackDeveloperWallets(tokenMint);
    return liquiditySafe && devSafe;
  }

  private async trailingStopLossMonitor(
    tokenAmount: TokenAmount,
    poolKeys: LiquidityPoolKeysV4,
    trailingPercent: number // e.g., 3 for 3%
  ): Promise<void> {
    let maxPrice: TokenAmount | null = null;
    while (true) {
      try {
        const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });
        const computed = Liquidity.computeAmountOut({
          poolKeys,
          poolInfo,
          amountIn: tokenAmount,
          currencyOut: this.config.quoteToken,
          slippage: new Percent(this.config.sellSlippage, 100),
        });
        const currentPrice = computed.amountOut as TokenAmount;

        if (!maxPrice || currentPrice.gt(maxPrice)) {
          maxPrice = currentPrice;
        }

        // Calculate trailing threshold: maxPrice * (1 - trailingPercent/100)
        const thresholdRaw = maxPrice.raw.mul(new BN(100 - trailingPercent * 100)).div(new BN(100));
        const thresholdPrice = new TokenAmount(this.config.quoteToken, thresholdRaw, true);

        // Log current metrics for debugging
        logger.debug({ mint: poolKeys.baseMint.toString() },
          `Max Price: ${maxPrice.toFixed()} | Current Price: ${currentPrice.toFixed()} | Threshold: ${thresholdPrice.toFixed()}`
        );

        if (currentPrice.lt(thresholdPrice)) {
          logger.warn({ mint: poolKeys.baseMint.toString() },
            `Trailing stop-loss triggered: current price ${currentPrice.toFixed()} below threshold ${thresholdPrice.toFixed()}. Initiating emergency sell.`
          );
          await this.forceSell(poolKeys, tokenAmount);
          break;
        }

        await sleep(this.MONITOR_INTERVAL);
      } catch (error) {
        logger.error({ mint: poolKeys.baseMint.toString(), error }, 'Error in trailing stop-loss monitor.');
        break;
      }
    }
  }

  private async forceSell(poolKeys: LiquidityPoolKeysV4, tokenAmount: TokenAmount): Promise<void> {
    try {
      logger.info({ mint: poolKeys.baseMint.toString() }, 'Executing emergency sell.');
      const result = await this.swap(
        poolKeys,
        this.config.wallet.publicKey,  // using wallet public key for accountId
        this.config.quoteAta,
        new Token(TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals),
        this.config.quoteToken,
        tokenAmount,
        this.config.sellSlippage,
        this.config.wallet,
        'sell'
      );
      if (result.confirmed) {
        logger.info({ mint: poolKeys.baseMint.toString(), signature: result.signature }, 'Emergency sell confirmed.');
      } else {
        logger.warn({ mint: poolKeys.baseMint.toString() }, 'Emergency sell failed.');
      }
    } catch (error) {
      logger.error({ mint: poolKeys.baseMint.toString(), error }, 'Error executing emergency sell.');
    }
  }

  // New method for partial profit-taking:
  private async partialProfitTakingMonitor(
    tokenAmount: TokenAmount,
    poolKeys: LiquidityPoolKeysV4,
    profitThreshold: number,    // e.g., 30 for 30% profit target
    sellPercentage: number      // e.g., 50 for selling 50% of position
  ): Promise<void> {
    // Get initial price
    const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });
    const initialComputed = Liquidity.computeAmountOut({
      poolKeys,
      poolInfo,
      amountIn: tokenAmount,
      currencyOut: this.config.quoteToken,
      slippage: new Percent(this.config.sellSlippage, 100),
    });
    const initialPrice = initialComputed.amountOut as TokenAmount;

    // Calculate target price
    const targetRaw = initialPrice.raw.mul(new BN(100 + profitThreshold)).div(new BN(100));
    let targetPrice = new TokenAmount(this.config.quoteToken, targetRaw, true);

    while (true) {
      try {
        const currentPoolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });
        const computed = Liquidity.computeAmountOut({
          poolKeys,
          poolInfo: currentPoolInfo,
          amountIn: tokenAmount,
          currencyOut: this.config.quoteToken,
          slippage: new Percent(this.config.sellSlippage, 100),
        });
        const currentPrice = computed.amountOut as TokenAmount;

        logger.debug(
          { mint: poolKeys.baseMint.toString() },
          `Partial profit monitor: Target ${targetPrice.toFixed()}, Current ${currentPrice.toFixed()}`
        );

        if (currentPrice.gt(targetPrice)) {
          // Calculate partial sell amount
          const partialRaw = tokenAmount.raw.mul(new BN(sellPercentage)).div(new BN(100));
          const partialSellAmount = new TokenAmount(tokenAmount.token, partialRaw, true);

          logger.info(
            { mint: poolKeys.baseMint.toString() },
            `Partial profit target reached. Selling ${sellPercentage}% of position.`
          );

          // Execute partial sell
          const result = await this.swap(
            poolKeys,
            this.config.wallet.publicKey,
            this.config.quoteAta,
            new Token(TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals),
            this.config.quoteToken,
            partialSellAmount,
            this.config.sellSlippage,
            this.config.wallet,
            'sell'
          );

          if (result.confirmed) {
            logger.info(
              { mint: poolKeys.baseMint.toString(), signature: result.signature },
              'Partial profit sell confirmed.'
            );
            
            // Update token amount for remaining position
            const remainingRaw = tokenAmount.raw.sub(partialRaw);
            tokenAmount = new TokenAmount(tokenAmount.token, remainingRaw, true);

            // Update target for remaining position
            const newTargetRaw = currentPrice.raw.mul(new BN(100 + profitThreshold)).div(new BN(100));
            targetPrice = new TokenAmount(this.config.quoteToken, newTargetRaw, true);
          }
        }

        await sleep(this.MONITOR_INTERVAL);
      } catch (error) {
        logger.error({ mint: poolKeys.baseMint.toString(), error }, 'Error in partial profit-taking monitor.');
        break;
      }
    }
  }


  private async getTokenBalance(tokenMint: PublicKey, connection: Connection): Promise<BigNumberish | null> {
    try {
      const tokenAccounts = await connection.getParsedTokenAccountsByOwner(
        this.config.wallet.publicKey,
        { mint: tokenMint }
      );

      if (tokenAccounts.value.length > 0) {
        const balance = tokenAccounts.value[0].account.data.parsed.info.tokenAmount.amount;
        return BigInt(balance); // Ensure it's a valid BigNumberish type
      }
    } catch (error) {
      console.error(`❌ Error fetching token balance:`, error);
    }
    return null;
  }

  private async getCachedData<T>(
    key: string,
    fetchFn: () => Promise<T>,
    ttlMs: number = 100
  ): Promise<T> {
    const cached = this.cache.get(key);
    const now = Date.now();
    
    if (cached && now - cached.timestamp < ttlMs) {
      return cached.data as T;
    }

    const fresh = await fetchFn();
    this.cache.set(key, {
      data: fresh,
      timestamp: now
    });
    
    return fresh;
  }

  private async getPoolInfo(poolKeys: LiquidityPoolKeysV4) {
    return this.getCachedData(
      `pool-${poolKeys.id}`,
      () => Liquidity.fetchInfo({ connection: this.connection, poolKeys }),
      100
    );
  }

  private async detectHoneypot(poolKeys: LiquidityPoolKeysV4): Promise<boolean> {
    try {
      // Create test amount as 1% of normal amount using BN arithmetic
      const ONE_PERCENT = new BN(1);
      const ONE_HUNDRED = new BN(100);
      const testAmount = new TokenAmount(
        this.config.quoteToken,
        this.config.quoteAmount.raw.mul(ONE_PERCENT).div(ONE_HUNDRED)
      );
      const poolInfo = await this.getPoolInfo(poolKeys);
      
      const buySimulation = Liquidity.computeAmountOut({
        poolKeys,
        poolInfo,
        amountIn: testAmount,
        currencyOut: this.config.quoteToken,
        slippage: new Percent(1, 100),
      });

      const sellSimulation = Liquidity.computeAmountOut({
        poolKeys,
        poolInfo,
        amountIn: buySimulation.amountOut as TokenAmount,
        currencyOut: this.config.quoteToken,
        slippage: new Percent(1, 100),
      });

      const roundTripLoss = testAmount.subtract((sellSimulation.amountOut as TokenAmount));
      const lossPercentage = new TokenAmount(
        this.config.quoteToken, 
        roundTripLoss.raw.mul(ONE_HUNDRED).div(testAmount.raw),
        true
      );

      // Return true if loss percentage is greater than 10%
      return lossPercentage.raw.toNumber() > 10;
    } catch (error) {
      logger.error('Honeypot detection failed:', error);
      return false;
    }
  }

  private async detectRugPullRisk(poolKeys: LiquidityPoolKeysV4): Promise<{
    safe: boolean;
    reason?: string;
  }> {
    try {
      const poolInfo = await this.getPoolInfo(poolKeys);
      
      // Validate LP token supply
      const lpTokenSupply = poolInfo.lpSupply;
      if (!lpTokenSupply || lpTokenSupply.isZero()) {
        return { safe: false, reason: 'Invalid LP token supply' };
      }

      // Safely get LP holders
      const largestLPHolders = await this.connection.getTokenLargestAccounts(poolKeys.lpMint);
      if (!largestLPHolders?.value?.length) {
        return { safe: false, reason: 'No LP holders found' };
      }
      console.log(`largestLPHolders: ${JSON.stringify(largestLPHolders)}`);

      // Calculate total concentration of top 10 holders
      const totalTopHoldersAmount = largestLPHolders.value
        .slice(0, 10) // Take top 10 holders
        .reduce((sum, holder) => sum + BigInt(holder.amount), BigInt(0));

      const concentration = (Number(totalTopHoldersAmount) / Number(lpTokenSupply)) * 100;
      
      logger.debug(
        `Top 10 holders concentration: ${concentration.toFixed(2)}% ` +
        `(${totalTopHoldersAmount} / ${lpTokenSupply})`
      );

      if (isNaN(concentration) || concentration > 50) {
        return { 
          safe: false, 
          reason: `High LP concentration: top 10 holders control ${concentration.toFixed(2)}%`
        };
      }

      // Check for recent liquidity changes
      const recentPoolChanges = await this.connection.getSignaturesForAddress(poolKeys.id);
      const suddenLiquidityRemoval = recentPoolChanges.some(tx => {
        if (!tx?.memo || !tx?.blockTime) return false;
        const timeSinceBlock = Date.now() - (tx.blockTime * 1000);
        return tx.memo.includes('remove_liquidity') && timeSinceBlock < 300000;
      });

      if (suddenLiquidityRemoval) {
        return { safe: false, reason: 'Recent suspicious liquidity removal' };
      }

      return { safe: true };
    } catch (error) {
      logger.error('Rug pull detection failed:', error);
      return { safe: false, reason: 'Detection error' };
    }
  }

  private async protectFromMEV(
    transaction: VersionedTransaction,
    wallet: Keypair
  ): Promise<VersionedTransaction> {
    try {
      // Add entropy to transaction without delay
      const dummyInstruction = SystemProgram.transfer({
        fromPubkey: wallet.publicKey,
        toPubkey: wallet.publicKey,
        lamports: Math.floor(Math.random() * 10) // Small random amount
      });

      // Decompile existing instructions
      const instructions = transaction.message.compiledInstructions.map(ix => ({
        programId: transaction.message.staticAccountKeys[ix.programIdIndex],
        keys: ix.accountKeyIndexes.map(idx => ({
          pubkey: transaction.message.staticAccountKeys[idx],
          isSigner: false,
          isWritable: false
        })),
        data: Buffer.from(ix.data)
      })) as TransactionInstruction[];

      // Add dummy instruction at the beginning to prevent front-running
      return new VersionedTransaction(
        new TransactionMessage({
          payerKey: wallet.publicKey,
          recentBlockhash: transaction.message.recentBlockhash,
          instructions: [dummyInstruction, ...instructions]
        }).compileToV0Message()
      );
    } catch (error) {
      logger.error('MEV protection failed:', error);
      return transaction; // Return original transaction if protection fails
    }
  }

  private async monitorTransaction(
    signature: string,
    context: { type: 'buy' | 'sell'; mint: string }
  ): Promise<void> {
    try {
      const tx = await this.connection.getParsedTransaction(signature, {
        maxSupportedTransactionVersion: 0,
        commitment: 'confirmed'
      });

      const metrics: TransactionMetrics = {
        timestamp: new Date().toISOString(),
        type: context.type,
        mint: context.mint,
        gasUsed: tx?.meta?.computeUnitsConsumed || 0,
        fee: tx?.meta?.fee || 0,
        success: !tx?.meta?.err,
        blockTime: tx?.blockTime ?? undefined,
        slot: tx?.slot ?? undefined,
      };

      // Only log metrics if transaction is confirmed
      if (metrics.success) {
        logger.info({ 
          transaction: metrics,
          url: `https://solscan.io/tx/${signature}?cluster=${this.connection.rpcEndpoint.includes('mainnet') ? 'mainnet' : 'devnet'}`
        }, 'Transaction metrics');

        // Only check gas usage for confirmed transactions
        if (metrics.gasUsed > 200000) {
          logger.warn({ signature, gasUsed: metrics.gasUsed }, 'High gas usage detected in confirmed transaction');
        }
      } else {
        logger.warn({ 
          signature,
          error: tx?.meta?.err 
        }, 'Transaction failed');
      }

      // Note: Implement metricsStorage if needed
      // await this.metricsStorage.save(metrics);

    } catch (error) {
      logger.error('Failed to monitor transaction:', error);
    }
  }

  private async quickRugCheck(
    poolKeys: LiquidityPoolKeysV4,
    tokenAmount: TokenAmount
  ): Promise<void> {
    const mintAddress = poolKeys.baseMint.toString();
    
    if (this.activeMonitors.has(mintAddress)) {
      clearInterval(this.activeMonitors.get(mintAddress)!.interval);
    }

    const initialCheck = await this.getQuickMetrics(poolKeys);
    const initialPrice = this.getTokenPrice(await this.getPoolInfo(poolKeys));
    
    const monitor = setInterval(async () => {
      try {
        const poolInfo = await this.getPoolInfo(poolKeys);
        const currentCheck = await this.getQuickMetrics(poolKeys);
        const currentPrice = this.getTokenPrice(poolInfo);
        const lastCheck = this.activeMonitors.get(mintAddress)?.lastCheck || initialCheck;

        // Calculate price change percentage
        const priceChangePercent = ((currentPrice - initialPrice) / initialPrice) * 100;

        // Quick checks for critical changes
        const lpRemovalPercent = ((lastCheck.lpConcentration - currentCheck.lpConcentration) / lastCheck.lpConcentration) * 100;
        const liquidityDropPercent = ((lastCheck.liquidityUSD - currentCheck.liquidityUSD) / lastCheck.liquidityUSD) * 100;

        // Emergency sell conditions
        if (
          lpRemovalPercent >= this.CRITICAL_THRESHOLDS.LP_REMOVAL_PERCENT ||
          currentCheck.liquidityUSD < this.CRITICAL_THRESHOLDS.MIN_LIQUIDITY_USD ||
          currentCheck.lpConcentration > this.CRITICAL_THRESHOLDS.MAX_LP_CONCENTRATION ||
          priceChangePercent <= -30 // Add 30% price drop threshold
        ) {
          logger.warn({
            mint: mintAddress,
            lpRemoval: lpRemovalPercent,
            liquidity: currentCheck.liquidityUSD,
            lpConcentration: currentCheck.lpConcentration,
            priceChange: priceChangePercent
          }, 'Critical risk detected - Emergency selling');

          await this.forceSell(poolKeys, tokenAmount);
          
          clearInterval(monitor);
          this.activeMonitors.delete(mintAddress);
          return;
        }

        this.activeMonitors.set(mintAddress, {
          interval: monitor,
          lastCheck: currentCheck
        });

      } catch (error) {
        logger.error({ mint: mintAddress, error }, 'Error in quick rug check');
      }
    }, this.QUICK_CHECK_INTERVAL);

    this.activeMonitors.set(mintAddress, {
      interval: monitor,
      lastCheck: initialCheck
    });
  }

  private async getQuickMetrics(poolKeys: LiquidityPoolKeysV4): Promise<QuickRugCheck> {
    try {
      const poolInfo = await this.getPoolInfo(poolKeys);
      
      // Validate pool info and LP supply
      if (!poolInfo?.lpSupply || poolInfo.lpSupply.isZero()) {
        logger.warn({ mint: poolKeys.baseMint.toString() }, 'Invalid or zero LP supply');
        return {
          lpConcentration: 100, // Assume worst case for safety
          liquidityUSD: 0,
          lastUpdate: Date.now()
        };
      }

      // Get and validate LP holders
      const lpHolders = await this.connection.getTokenLargestAccounts(poolKeys.lpMint);
      if (!lpHolders?.value?.length) {
        logger.warn({ mint: poolKeys.baseMint.toString() }, 'No LP holders found');
        return {
          lpConcentration: 100, // Assume worst case for safety
          liquidityUSD: 0,
          lastUpdate: Date.now()
        };
      }

      // Calculate concentration with validation
      const largestHolderAmount = lpHolders.value[0].amount;
      if (!largestHolderAmount) {
        logger.warn({ mint: poolKeys.baseMint.toString() }, 'Invalid LP holder amount');
        return {
          lpConcentration: 100, // Assume worst case for safety
          liquidityUSD: 0,
          lastUpdate: Date.now()
        };
      }

      const lpConcentration = (Number(largestHolderAmount) / Number(poolInfo.lpSupply)) * 100;
      
      // Validate concentration calculation
      if (isNaN(lpConcentration) || !isFinite(lpConcentration)) {
        logger.warn({ mint: poolKeys.baseMint.toString() }, 'Invalid LP concentration calculation');
        return {
          lpConcentration: 100, // Assume worst case for safety
          liquidityUSD: 0,
          lastUpdate: Date.now()
        };
      }

      // Quick liquidity check in USD
      const liquidityUSD = this.calculatePoolLiquidityUSD(poolInfo);

      return {
        lpConcentration,
        liquidityUSD,
        lastUpdate: Date.now()
      };

    } catch (error) {
      logger.error({ 
        mint: poolKeys.baseMint.toString(),
        error 
      }, 'Error calculating quick metrics');
      
      // Return conservative values on error
      return {
        lpConcentration: 100, // Assume worst case for safety
        liquidityUSD: 0,
        lastUpdate: Date.now()
      };
    }
  }

  private calculatePoolLiquidityUSD(poolInfo: any): number {
    try {
      // Input validation
      if (!poolInfo?.baseReserve || !poolInfo?.quoteReserve) {
        logger.warn('Missing pool reserves');
        return 0;
      }

      // Convert to BN and validate
      const baseReserve = new BN(poolInfo.baseReserve);
      const quoteReserve = new BN(poolInfo.quoteReserve);
      
      if (baseReserve.isZero() || quoteReserve.isZero()) {
        logger.debug('Zero reserves detected');
        return 0;
      }

      // Get decimals with validation
      const baseDecimal = Number(poolInfo.baseDecimals) || 9;
      const quoteDecimal = Number(poolInfo.quoteDecimals) || 6;
      
      if (isNaN(baseDecimal) || isNaN(quoteDecimal)) {
        logger.warn('Invalid decimal values');
        return 0;
      }

      // Calculate amounts using BN arithmetic to maintain precision
      const baseAmountBN = baseReserve.div(new BN(10).pow(new BN(baseDecimal)));
      const quoteAmountBN = quoteReserve.div(new BN(10).pow(new BN(quoteDecimal)));

      if (baseAmountBN.isZero() || quoteAmountBN.isZero()) {
        logger.debug('Zero amounts after decimal adjustment');
        return 0;
      }

      // Calculate price using BN division
      const priceBN = quoteAmountBN.mul(new BN(10).pow(new BN(9))).div(baseAmountBN);
      const price = priceBN.toNumber() / 1e9;

      if (isNaN(price) || price <= 0) {
        logger.warn('Invalid price calculation');
        return 0;
      }

      // Calculate total liquidity in USD
      // sqrt(baseAmount * quoteAmount * 4) gives us the geometric mean of both sides
      const baseInQuote = baseAmountBN.mul(priceBN).div(new BN(10).pow(new BN(9)));
      const totalLiquidityBN = baseInQuote.add(quoteAmountBN);
      const totalLiquidityUSD = totalLiquidityBN.toNumber() / 1e6; // Convert to USD assuming 6 decimals

      // Final validation
      if (isNaN(totalLiquidityUSD) || totalLiquidityUSD < 0) {
        logger.warn('Invalid liquidity calculation');
        return 0;
      }

      logger.debug({
        baseReserve: baseReserve.toString(),
        quoteReserve: quoteReserve.toString(),
        price,
        liquidityUSD: totalLiquidityUSD
      }, 'Pool liquidity calculation');

      return totalLiquidityUSD;

    } catch (error) {
      logger.error({
        error,
        poolInfo
      }, 'Error calculating pool liquidity');
      return 0;
    }
  }

  private getTokenPrice(poolInfo: any): number {
    try {
      // Input validation
      if (!poolInfo?.baseReserve || !poolInfo?.quoteReserve) {
        logger.debug('Missing pool reserves');
        return 0;
      }

      const baseReserve = new BN(poolInfo.baseReserve);
      const quoteReserve = new BN(poolInfo.quoteReserve);
      
      // Early validation
      if (baseReserve.isZero() || quoteReserve.isZero()) {
        logger.debug('Zero reserves detected');
        return 0;
      }

      // Safe decimal handling
      const baseDecimal = Number(poolInfo.baseDecimals) || 9;
      const quoteDecimal = Number(poolInfo.quoteDecimals) || 6;
      
      if (isNaN(baseDecimal) || isNaN(quoteDecimal)) {
        logger.warn('Invalid decimal values');
        return 0;
      }

      // Convert to decimal numbers with validation
      const baseAmount = Number(baseReserve) / Math.pow(10, baseDecimal);
      if (isNaN(baseAmount) || baseAmount <= 0) {
        logger.debug('Invalid base amount calculation');
        return 0;
      }

      const quoteAmount = Number(quoteReserve) / Math.pow(10, quoteDecimal);
      if (isNaN(quoteAmount) || quoteAmount <= 0) {
        logger.debug('Invalid quote amount calculation');
        return 0;
      }

      const price = quoteAmount / baseAmount;
      
      // Validate final price
      if (!isFinite(price) || price < this.MIN_VALID_PRICE) {
        logger.debug('Invalid price calculation result');
        return 0;
      }

      return price;

    } catch (error) {
      logger.error('Error calculating token price:', error);
      return 0;
    }
  }
}
