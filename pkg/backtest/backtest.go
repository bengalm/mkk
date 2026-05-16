package backtest

import (
	"fmt"
	"math"
	"time"

	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/rs/zerolog/log"
)

// Result holds backtest results.
type Result struct {
	Strategy       string         `json:"strategy"`
	Pair           string         `json:"pair"`
	Timeframe      string         `json:"timeframe"`
	StartTime      time.Time      `json:"start_time"`
	EndTime        time.Time      `json:"end_time"`
	InitialBalance float64        `json:"initial_balance"`
	FinalBalance   float64        `json:"final_balance"`
	TotalTrades    int            `json:"total_trades"`
	WinTrades      int            `json:"win_trades"`
	LossTrades     int            `json:"loss_trades"`
	WinRate        float64        `json:"win_rate"`
	MaxDrawdown    float64        `json:"max_drawdown"`
	SharpeRatio    float64        `json:"sharpe_ratio"`
	ProfitFactor   float64        `json:"profit_factor"`
	TotalPnL       float64        `json:"total_pnl"`
	Returns        []PeriodReturn `json:"returns"`
	Trades         []TradeRecord  `json:"trades"`
}

// PeriodReturn holds periodic return data.
type PeriodReturn struct {
	Time   time.Time `json:"time"`
	Value  float64   `json:"value"`  // portfolio value
	Return float64   `json:"return"` // period return %
}

// TradeRecord holds a single trade record.
type TradeRecord struct {
	Time     time.Time         `json:"time"`
	Side     exchange.OrderSide `json:"side"`
	Price    float64           `json:"price"`
	Amount   float64           `json:"amount"`
	PnL      float64           `json:"pnl"`
	Reason   string            `json:"reason"`
	Balance  float64           `json:"balance"`
}

// Engine runs backtests.
type Engine struct {
	balance    float64
	position   *simPosition
	trades     []TradeRecord
	peakValue  float64
	maxDrawdown float64
	totalGain  float64
	totalLoss  float64
}

type simPosition struct {
	Side       exchange.OrderSide
	EntryPrice float64
	Amount     float64
}

// NewEngine creates a new backtest engine.
func NewEngine(initialBalance float64) *Engine {
	return &Engine{
		balance:   initialBalance,
		peakValue: initialBalance,
		trades:    make([]TradeRecord, 0),
	}
}

// Run executes a backtest with the given candles and strategy.
func (e *Engine) Run(candles []exchange.Candle, s strategy.Strategy) *Result {
	if len(candles) == 0 {
		return nil
	}

	s.Init(make(map[string]interface{}), &mockExchange{}, nil)

	// Create a goroutine to consume signals from the strategy
	sigDone := make(chan struct{})
	go func() {
		defer close(sigDone)
		// Try to get signal channel via type assertion on concrete types
		// Backtest will process signals via OnCandle callbacks
	}()

	for i, candle := range candles {
		s.OnCandle(candle)
		s.OnTick(exchange.Ticker{
			Pair:      candle.Pair,
			Last:      candle.Close,
			Timestamp: candle.Timestamp,
		})

		// Update peak and drawdown
		currentValue := e.portfolioValue(candle.Close)
		if currentValue > e.peakValue {
			e.peakValue = currentValue
		}
		drawdown := (e.peakValue - currentValue) / e.peakValue * 100
		if drawdown > e.maxDrawdown {
			e.maxDrawdown = drawdown
		}

		// Log progress every 1000 candles
		if i > 0 && i%1000 == 0 {
			log.Debug().
				Int("candle", i).
				Int("total", len(candles)).
				Float64("balance", e.balance).
				Msg("Backtest progress")
		}
	}

	// Close any remaining position
	if e.position != nil && len(candles) > 0 {
		lastPrice := candles[len(candles)-1].Close
		e.closePosition(lastPrice, candles[len(candles)-1].Timestamp, "backtest end")
	}

	<-sigDone
	return e.buildResult(s.Name(), candles)
}

func (e *Engine) processSignal(sig strategy.TradeSignal, ts time.Time) {
	switch sig.Action {
	case strategy.ActionBuy:
		if e.position != nil {
			return // already in position
		}
		cost := sig.Amount * sig.Price
		if cost > e.balance {
			sig.Amount = e.balance / sig.Price * 0.99 // leave margin
		}
		e.position = &simPosition{
			Side:       exchange.Buy,
			EntryPrice: sig.Price,
			Amount:     sig.Amount,
		}
		e.balance -= sig.Amount * sig.Price

	case strategy.ActionSell:
		if e.position != nil {
			return
		}
		e.position = &simPosition{
			Side:       exchange.Sell,
			EntryPrice: sig.Price,
			Amount:     sig.Amount,
		}
		e.balance -= sig.Amount * sig.Price

	case strategy.ActionCloseLong, strategy.ActionCloseShort:
		e.closePosition(sig.Price, ts, sig.Reason)
	}
}

func (e *Engine) closePosition(price float64, ts time.Time, reason string) {
	if e.position == nil {
		return
	}

	var pnl float64
	if e.position.Side == exchange.Buy {
		pnl = (price - e.position.EntryPrice) * e.position.Amount
	} else {
		pnl = (e.position.EntryPrice - price) * e.position.Amount
	}

	e.balance += e.position.Amount*price + pnl

	trade := TradeRecord{
		Time:    ts,
		Side:    e.position.Side,
		Price:   price,
		Amount:  e.position.Amount,
		PnL:     math.Round(pnl*100) / 100,
		Reason:  reason,
		Balance: math.Round(e.balance*100) / 100,
	}
	e.trades = append(e.trades, trade)

	if pnl > 0 {
		e.totalGain += pnl
	} else {
		e.totalLoss += math.Abs(pnl)
	}

	e.position = nil
}

func (e *Engine) portfolioValue(currentPrice float64) float64 {
	val := e.balance
	if e.position != nil {
		if e.position.Side == exchange.Buy {
			val += e.position.Amount * currentPrice
		} else {
			val += e.position.Amount * (2*e.position.EntryPrice - currentPrice)
		}
	}
	return val
}

func (e *Engine) buildResult(name string, candles []exchange.Candle) *Result {
	wins := 0
	losses := 0
	for _, t := range e.trades {
		if t.PnL > 0 {
			wins++
		} else if t.PnL < 0 {
			losses++
		}
	}

	winRate := 0.0
	if wins+losses > 0 {
		winRate = float64(wins) / float64(wins+losses) * 100
	}

	profitFactor := 0.0
	if e.totalLoss > 0 {
		profitFactor = e.totalGain / e.totalLoss
	}

	pnl := e.balance - e.peakValue // simplified

	pair := ""
	tf := ""
	if len(candles) > 0 {
		pair = candles[0].Pair
		tf = candles[0].Timeframe
	}

	return &Result{
		Strategy:       name,
		Pair:           pair,
		Timeframe:      tf,
		StartTime:      candles[0].Timestamp,
		EndTime:        candles[len(candles)-1].Timestamp,
		InitialBalance: e.peakValue,
		FinalBalance:   e.balance,
		TotalTrades:    len(e.trades),
		WinTrades:      wins,
		LossTrades:     losses,
		WinRate:        math.Round(winRate*100) / 100,
		MaxDrawdown:    math.Round(e.maxDrawdown*100) / 100,
		ProfitFactor:   math.Round(profitFactor*100) / 100,
		TotalPnL:       math.Round(pnl*100) / 100,
		Trades:         e.trades,
	}
}

// PrintResult prints a formatted backtest report.
func PrintResult(r *Result) {
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Backtest Report: %s on %s (%s)\n", r.Strategy, r.Pair, r.Timeframe)
	fmt.Println("═══════════════════════════════════════════")
	fmt.Printf("  Period:      %s → %s\n", r.StartTime.Format("2006-01-02"), r.EndTime.Format("2006-01-02"))
	fmt.Printf("  Initial:     $%.2f\n", r.InitialBalance)
	fmt.Printf("  Final:       $%.2f\n", r.FinalBalance)
	fmt.Printf("  P&L:         $%.2f (%.2f%%)\n", r.TotalPnL, (r.FinalBalance/r.InitialBalance-1)*100)
	fmt.Println("───────────────────────────────────────────")
	fmt.Printf("  Total Trades: %d\n", r.TotalTrades)
	fmt.Printf("  Wins:         %d  |  Losses: %d\n", r.WinTrades, r.LossTrades)
	fmt.Printf("  Win Rate:     %.1f%%\n", r.WinRate)
	fmt.Printf("  Max Drawdown: %.2f%%\n", r.MaxDrawdown)
	fmt.Printf("  Profit Factor: %.2f\n", r.ProfitFactor)
	fmt.Println("═══════════════════════════════════════════")
}

// mockExchange is a minimal exchange mock for backtesting.
type mockExchange struct{}

func (m *mockExchange) GetTicker(pair string) (*exchange.Ticker, error) { return nil, nil }
func (m *mockExchange) GetCandles(req exchange.CandleRequest) ([]exchange.Candle, error) {
	return nil, nil
}
func (m *mockExchange) GetOrderBook(pair string, depth int) (*exchange.OrderBook, error) {
	return nil, nil
}
func (m *mockExchange) GetBalance(currencies ...string) ([]exchange.Balance, error) {
	return nil, nil
}
func (m *mockExchange) GetPositions(pairs ...string) ([]exchange.Position, error) {
	return nil, nil
}
func (m *mockExchange) PlaceOrder(req exchange.OrderRequest) (*exchange.Order, error) {
	return &exchange.Order{ID: "mock", Status: exchange.StatusFilled}, nil
}
func (m *mockExchange) CancelOrder(pair, orderID string) error { return nil }
func (m *mockExchange) GetOrder(pair, orderID string) (*exchange.Order, error) {
	return nil, nil
}
func (m *mockExchange) GetOpenOrders(pair string) ([]exchange.Order, error) { return nil, nil }
func (m *mockExchange) SubscribeTicker(pair string, handler func(exchange.Ticker)) error {
	return nil
}
func (m *mockExchange) SubscribeCandles(pair, timeframe string, handler func(exchange.Candle)) error {
	return nil
}
func (m *mockExchange) SubscribeOrderBook(pair string, handler func(exchange.OrderBook)) error {
	return nil
}
func (m *mockExchange) Close() error { return nil }
