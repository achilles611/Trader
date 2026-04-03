# ETH Trading Bot Starter

This project is a conservative starter bot for **Ethereum spot trading**. It is built to help you test a strategy with paper money first, not to promise returns.

## Reality Check

A goal of **10% gains every 2 hours** is not a realistic steady trading target. At that pace, losses, fees, slippage, outages, and market gaps will catch up fast. The SEC's investor education site warns that crypto asset investments are exceptionally volatile and speculative, and Coinbase notes that downside protection is not guaranteed during high market volatility.

Use this as a paper-trading and research bot first.

## What It Does

- Pulls public ETH candles from Coinbase.
- Uses pullback-resume entries with EMA trend checks, RSI filters, directional pullback detection, optional market-state gating, and per-profile weighted scoring.
- Can optionally take paper-trading short positions on bearish momentum.
- Sizes positions with a fixed risk budget, aggressiveness scaling, and hard size clamps.
- Manages stop loss, take profit, market-state-aware trailing stops, chop profit locks, cooldowns, flip guards, trade-rate limits, stall exits, and a max-duration watchdog.
- Includes circuit breakers for drawdown, consecutive losses, trade count, and a manual kill switch file.
- Includes a baseline neural scorer (`24 -> 32 -> 24 -> 16 -> 8 -> 2`) that scores long-vs-short win probability from the rule feature vector.
- Can run a 10-instance paper-trading swarm with isolated state, per-instance logs, per-instance network snapshots, SVG visualizations, and next-generation profile proposals.
- Runs in `paper` mode by default.
- Can place Coinbase live market orders only if you explicitly switch to `BOT_MODE=live` and provide API keys.
- Live shorting is not enabled in this starter because the current adapter is built around Coinbase spot orders.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

## Commands

All commands below also work through the module shim, for example `python -m trader.cli swarm-session --minutes 60 --generation 1`.

Run one cycle:

```powershell
python main.py once
```

Run continuously:

```powershell
python main.py run
```

Run a timed forward-paper session and save a report:

```powershell
python main.py session --minutes 60 --report-file logs\paper_session_report.json
```

Run a backtest on recent candles:

```powershell
python main.py backtest --candles 1000
```

Run the 10-bot swarm for one generation:

```powershell
python main.py swarm-session --minutes 60 --generation 1
```

Train the baseline network from captured trade samples:

```powershell
python main.py train-network --input logs\training\trade_samples.jsonl --epochs 12
```

Emit next-generation mutation proposals:

```powershell
python main.py evolve --from-generation 1 --to-generation 2
```

Render one instance network bundle:

```powershell
python main.py viz-network --instance tr4 --generation 1
```

Dump the current swarm profiles:

```powershell
python main.py profile-dump --generation 1
```

Run the production orchestration cycle wrapper:

```powershell
python -m src.orchestrator run-cycle
```

Check orchestration health:

```powershell
python -m src.orchestrator health-check
```

Replay AI analysis from a saved cycle bundle:

```powershell
python -m src.orchestrator replay-analysis --bundle artifacts\cycles\YYYY\MM\DD\cycle_...\cycle_bundle.json
```

## Live Trading Warning

If you enable live mode:

- Start with tiny size.
- Use a dedicated API key with the minimum required permissions.
- Watch the first several runs manually.
- Understand that this starter bot uses polling and **does not guarantee exchange-side stop execution** if your machine, internet, or process goes down.

## Key Config

The main knobs live in `.env`:

- `BOT_MODE`: `paper` or `live`
- `BOT_TRADING_ENABLED`: startup-level manual kill switch
- `BOT_PRODUCT_ID`: default `ETH-USD`
- `BOT_AGGRESSIVENESS`: scales calculated position size without changing the signal logic
- `BOT_ENABLE_SHORTS`: enable paper-trading short entries
- `BOT_AGGRESSIVE_ENTRIES`: allow continuation entries instead of waiting only for fresh crosses
- `BOT_MAX_CONCURRENT_TRADES`: forward-looking concurrency knob; the current runtime still manages one open position at a time
- `BOT_MIN_CONFIRMATION_SIGNALS`: how many strong pullback/trend signals are needed before entry
- `BOT_PULLBACK_LOOKBACK_CANDLES` / `BOT_PULLBACK_MIN_PCT`: pullback detection window and minimum depth
- `BOT_LONG_TOP_GUARD_PCT` / `BOT_SHORT_BOTTOM_GUARD_PCT`: block longs near recent highs and shorts near recent lows
- `BOT_MARKET_STATE_LOOKBACK_CANDLES` / `BOT_MARKET_TREND_EFFICIENCY_THRESHOLD`: trend-vs-chop classifier inputs
- `BOT_BLOCK_ENTRIES_IN_CHOP`: optionally hard-gates trend-resume entries unless the market is classified as `TRENDING`
- `BOT_CHOP_PROFIT_LOCK_TRIGGER_PCT` / `BOT_CHOP_PROFIT_LOCK_STOP_BUFFER_PCT`: moves the stop past breakeven once a chop trade gets favorable movement
- `BOT_CHOP_STALL_MINUTES` / `BOT_CHOP_STALL_EXIT_BAND_PCT`: early-exit stale chop trades before the full duration watchdog
- `BOT_MAX_POSITION_SIZE` / `BOT_MIN_POSITION_SIZE`: quote-currency position clamp
- `BOT_COOLDOWN_AFTER_LOSS_SECONDS` / `BOT_COOLDOWN_AFTER_WIN_SECONDS`: anti-churn cooldowns
- `BOT_FLIP_COOLDOWN_SECONDS`: blocks immediate long-to-short or short-to-long flips
- `BOT_MAX_TRADES_TOTAL` / `BOT_MAX_TRADES_PER_HOUR`: overtrading protection
- `BOT_MAX_DRAWDOWN_PCT` / `BOT_MAX_CONSECUTIVE_LOSSES`: circuit breakers
- `BOT_MAX_TRADE_DURATION_MINUTES`: force-exit watchdog for stuck trades
- `BOT_MAX_SPREAD_THRESHOLD`: public-data candle-range proxy for unstable entries
- `BOT_MIN_EXPECTED_MOVE_MULTIPLE`: rejects entries when the target move does not cover modeled fees/slippage
- `BOT_KILL_SWITCH_PATH`: if this file exists, new entries are blocked immediately
- `BOT_TRAINING_SAMPLE_LOG_PATH`: shared JSONL sink for per-trade training samples
- `BOT_BASELINE_NETWORK_PATH`: baseline network snapshot used to seed per-instance network clones
- `BOT_RISK_PER_TRADE_PCT`: fraction of equity risked on each trade
- `BOT_STOP_LOSS_PCT`: hard stop distance
- `BOT_TAKE_PROFIT_PCT`: profit target
- `BOT_TRAILING_STOP_PCT`: fallback trailing stop distance
- `BOT_TRAILING_STOP_PCT_TRENDING` / `BOT_TRAILING_STOP_PCT_CHOPPY`: trend-aware trailing stop distances
- `BOT_DAILY_MAX_LOSS_PCT`: block new entries after a bad day
- `BOT_MAX_NOTIONAL_PCT`: caps how much cash any one trade can use
- `BOT_SHORT_RSI_ENTRY_FLOOR` / `BOT_SHORT_RSI_ENTRY_CEILING`: bearish RSI filter for short entries

## Logging

- Each closed trade is appended to `logs/trades.jsonl` with timestamp, direction, entry/exit, position size, entry reason, `entry_quality_score`, indicator snapshot, market state, reason tag, result, P&L, and trade duration.
- Each signal evaluation is appended to `logs/signals.jsonl` with the candidate action, market state, `entry_quality_score`, indicators, and any explicit block reason such as `blocked_choppy_market`.
- Each timed `session` run emits a JSON summary with total trades, wins, losses, win rate, max drawdown, final P&L, and halt reason.
- Each closed trade also emits a training row to `logs/training/trade_samples.jsonl` with the exact entry feature vector, network scores, fee-aware/raw labels, holding time, and excursion stats.
- Swarm runs write isolated artifacts per instance under `state/instances/`, `logs/instances/`, `reports/generation_*/instances/`, `models/instances/`, and `viz/instances/`.

## Swarm Overview

- `zerk1` and `zerk2` are reckless exploration agents that bias toward fast momentum and contrarian scalp discovery.
- `tr1` through `tr8` are structured descendants of the current rule engine with different weighting, thresholds, and network trust.
- All swarm bots share one Coinbase market frame per cycle, then decide independently with isolated capital buckets and isolated state/log paths.
- After a swarm session, the orchestrator emits `reports/generation_XXX/swarm_session_report.json` and `reports/generation_XXX/next_generation_proposals.json`.

## Production Wrapper

- `config/global.yaml` and `config/bots/*.yaml` define the 30-minute orchestration loop, guardrails, and per-bot templates.
- `src/orchestrator.py` adds repo sync, run locking, SQLite persistence, artifact writing, OpenAI Responses analysis, and branch-only patch request staging.
- `scripts/bootstrap_server.sh` installs the app on Ubuntu and enables `deploy/systemd/trader-swarm.timer`.

## Sources

- [Coinbase Advanced Trade REST SDK quickstart](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/guides/sdk-rest-api)
- [Coinbase order management guide](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/guides/orders)
- [SEC Investor.gov crypto investor alert](https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-alerts/crypto-asset-securities)
