import {
  ComputeBudgetProgram,
  Connection,
  Keypair,
  PublicKey,
  TransactionMessage,
  VersionedTransaction,
} from '@solana/web3.js';
import {
  createAssociatedTokenAccountIdempotentInstruction,
  createCloseAccountInstruction,
  getAccount,
  getAssociatedTokenAddress,
  RawAccount,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import { Liquidity, LiquidityPoolKeysV4, LIQUIDITY_STATE_LAYOUT_V4, LiquidityStateV4, Percent, Token, TokenAmount } from '@raydium-io/raydium-sdk';
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

export class Bot {
  private readonly poolFilters: PoolFilters;
  private readonly snipeListCache?: SnipeListCache;
  private readonly mutex: Mutex;
  private sellExecutionCount = 0;
  public readonly isWarp: boolean = false;
  public readonly isJito: boolean = false;

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
      // Adaptive slippage: compute dynamic buy slippage based on market volatility
      const volatility = await this.getMarketVolatility(poolState.baseMint);
      const dynamicBuySlippage = this.calculateAdaptiveSlippage(volatility);
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
            dynamicBuySlippage,
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
        // Calculate threshold price: maxPrice * (1 - trailingPercent)
        const thresholdRaw = maxPrice.raw
          .mul(new BN(100 - trailingPercent * 100))
          .div(new BN(100));
        const thresholdPrice = new TokenAmount(this.config.quoteToken, thresholdRaw, true);
        if (currentPrice.lt(thresholdPrice)) {
          logger.warn(
            { mint: poolKeys.baseMint.toString() },
            `Trailing stop-loss triggered: current price ${currentPrice.toFixed()} is below threshold ${thresholdPrice.toFixed()}. Initiating emergency sell.`
          );
          await this.forceSell(poolKeys, tokenAmount);
          break;
        }
        await sleep(this.config.priceCheckInterval);
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

  /*
    partialProfitThreshold: number;
    partialProfitSellPercent: number;
  */

  // New method for partial profit-taking:
  private async partialProfitTakingMonitor(
    purchasedTokenAmount: TokenAmount,
    poolKeys: LiquidityPoolKeysV4,
    profitThreshold: number,    // e.g., 20 for 20% profit target
    sellPercentage: number      // e.g., 30 for selling 30% of position
  ): Promise<void> {
    // Compute the initial price based on the purchased amount.
    // Here we assume that when you bought, the quote amount (in your config) represents the initial price.
    // You might replace this with a more precise purchase price.
    const initialPrice = this.config.quoteAmount;
    // Compute target price = initialPrice * (1 + profitThreshold/100)
    const targetRaw = initialPrice.raw.mul(new BN(100 + profitThreshold)).div(new BN(100));
    const targetPrice = new TokenAmount(this.config.quoteToken, targetRaw, true);

    while (true) {
      try {
        const poolInfo = await Liquidity.fetchInfo({ connection: this.connection, poolKeys });
        const computed = Liquidity.computeAmountOut({
          poolKeys,
          poolInfo,
          amountIn: purchasedTokenAmount,
          currencyOut: this.config.quoteToken,
          slippage: new Percent(this.config.sellSlippage, 100),
        });
        const currentPrice = computed.amountOut as TokenAmount;
        logger.debug(
          { mint: poolKeys.baseMint.toString() },
          `Partial profit monitor: Target ${targetPrice.toFixed()}, Current ${currentPrice.toFixed()}`
        );
        if (currentPrice.gt(targetPrice)) {
          // Sell a fraction of your position
          const partialRaw = purchasedTokenAmount.raw.mul(new BN(sellPercentage)).div(new BN(100));
          const partialSellAmount = new TokenAmount(purchasedTokenAmount.token, partialRaw, true);
          logger.info(
            { mint: poolKeys.baseMint.toString() },
            `Partial profit target reached. Selling ${sellPercentage}% of position.`
          );
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
          } else {
            logger.warn({ mint: poolKeys.baseMint.toString() }, 'Partial profit sell failed.');
          }
          break;
        }
        await sleep(this.config.priceCheckInterval);
      } catch (error) {
        logger.error({ mint: poolKeys.baseMint.toString(), error }, 'Error in partial profit-taking monitor.');
        break;
      }
    }
  }

}
