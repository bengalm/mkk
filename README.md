# MKK — Crypto Trading Bot (Go + OKX)

> A high-performance cryptocurrency trading bot built with Go, featuring backtesting, paper trading, live trading, and a web dashboard.

## Features

- **3 Built-in Strategies**: Grid, DCA, RSI (extensible)
- **OKX Exchange**: Full REST + WebSocket support
- **Backtesting**: Historical candle replay with performance metrics
- **Paper Trading**: Simulated trading with realistic fills
- **Live Trading**: Real-time signal execution with risk management
- **Risk Management**: Position limits, daily loss caps, drawdown protection, R:R filtering
- **Web Dashboard**: Real-time P&L, positions, trade history
- **Event-Driven**: Pub/sub event bus for decoupled architecture
- **Data Persistence**: SQLite for trades, positions, candles, backtest results

## Quick Start

```bash
# Clone
git clone https://github.com/bengalm/mkk.git
cd mkk

# Build
go build -o mkk ./cmd/mkk

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your OKX API keys

# List strategies
./mkk strategies

# Backtest RSI strategy
./mkk backtest rsi --pair BTC-USDT --timeframe 1h --balance 10000

# Paper trade
./mkk paper rsi --balance 10000

# Live trade (CAUTION!)
./mkk trade rsi

# Web dashboard
./mkk serve --port 8080
```

## Architecture

```
cmd/mkk/          CLI entry point (cobra)
pkg/
  config/         YAML configuration loader
  logger/         zerolog structured logging
  eventbus/       Pub/sub event bus
  exchange/
    types.go      Exchange interface & data types
    okx/          OKX REST + WebSocket implementation
  indicator/      Technical indicators (SMA, EMA, RSI, MACD, BB, ATR, Stoch)
  strategy/
    strategy.go   Strategy interface, Registry, Manager
    grid/         Grid trading strategy
    dca/          Dollar-cost averaging strategy
    rsi/          RSI crossover strategy
  backtest/       Backtesting engine with performance metrics
  paper/          Paper trading engine (simulated)
  trader/         Live trading engine with risk management
  repository/     SQLite persistence layer
internal/
  web/            HTTP API + dashboard
```

## Strategy Interface

Implement the `Strategy` interface to create custom strategies:

```go
type Strategy interface {
    Name() string
    Init(config map[string]interface{}, ex Exchange, bus *EventBus) error
    OnTick(ticker Ticker)
    OnCandle(candle Candle)
    OnFill(trade Trade)
    Stop()
    IsActive() bool
}
```

Register with `init()`:
```go
func init() {
    strategy.Register("my-strategy", func() strategy.Strategy {
        return &MyStrategy{}
    })
}
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | Engine status & stats |
| `GET /api/positions` | Open positions |
| `GET /api/trades` | Trade history |
| `GET /api/pnl` | Daily P&L summary |
| `GET /api/balance` | Balance & equity |
| `GET /api/strategies` | Available strategies |
| `GET /api/candles?pair=BTC-USDT&timeframe=1h` | Candle data |
| `GET /` | Web dashboard |

## Risk Management

- **Max Position Size**: Limit USDT per trade
- **Daily Loss Limit**: Stop trading after hitting daily loss cap
- **Max Drawdown**: Auto-shutdown on excessive drawdown
- **R:R Filter**: Reject signals below minimum risk/reward ratio
- **Max Open Positions**: Limit concurrent positions

## Configuration

See [config.example.yaml](config.example.yaml) for full configuration options.

## License

MIT
