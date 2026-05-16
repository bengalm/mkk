package paper

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/rs/zerolog/log"
)

// PaperEngine simulates trading without real orders.
type PaperEngine struct {
	mu          sync.RWMutex
	balance     float64
	initial     float64
	feeRate     float64
	slippage    float64
	positions   map[string]*PaperPosition
	trades      []PaperTrade
	orderIDSeq  int
	tickerCache map[string]exchange.Ticker
}

// PaperPosition holds a simulated position.
type PaperPosition struct {
	Pair       string            `json:"pair"`
	Side       exchange.OrderSide `json:"side"`
	EntryPrice float64           `json:"entry_price"`
	Amount     float64           `json:"amount"`
	OpenTime   time.Time         `json:"open_time"`
	StopLoss   float64           `json:"stop_loss,omitempty"`
	TakeProfit float64           `json:"take_profit,omitempty"`
}

// PaperTrade records a simulated trade.
type PaperTrade struct {
	ID        string             `json:"id"`
	Pair      string             `json:"pair"`
	Side      exchange.OrderSide `json:"side"`
	Price     float64            `json:"price"`
	Amount    float64            `json:"amount"`
	PnL       float64            `json:"pnl"`
	Fee       float64            `json:"fee"`
	Reason    string             `json:"reason"`
	Timestamp time.Time          `json:"timestamp"`
	Balance   float64            `json:"balance"`
}

// NewPaperEngine creates a new paper trading engine.
func NewPaperEngine(initialBalance, feeRate, slippage float64) *PaperEngine {
	return &PaperEngine{
		balance:     initialBalance,
		initial:     initialBalance,
		feeRate:     feeRate,
		slippage:    slippage,
		positions:   make(map[string]*PaperPosition),
		trades:      make([]PaperTrade, 0),
		tickerCache: make(map[string]exchange.Ticker),
	}
}

// UpdateTicker updates the cached ticker price.
func (p *PaperEngine) UpdateTicker(ticker exchange.Ticker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tickerCache[ticker.Pair] = ticker
}

// OpenPosition opens a simulated position.
func (p *PaperEngine) OpenPosition(pair string, side exchange.OrderSide, amount, price, stopLoss, takeProfit float64) (*PaperTrade, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.positions[pair]; exists {
		return nil, fmt.Errorf("already have position on %s", pair)
	}

	// Apply slippage
	fillPrice := price
	if p.slippage > 0 {
		if side == exchange.Buy {
			fillPrice = price * (1 + p.slippage)
		} else {
			fillPrice = price * (1 - p.slippage)
		}
	}

	cost := amount * fillPrice
	fee := cost * p.feeRate

	if cost+fee > p.balance {
		return nil, fmt.Errorf("insufficient balance: need %.2f, have %.2f", cost+fee, p.balance)
	}

	p.orderIDSeq++
	trade := PaperTrade{
		ID:        fmt.Sprintf("PAPER-%05d", p.orderIDSeq),
		Pair:      pair,
		Side:      side,
		Price:     fillPrice,
		Amount:    amount,
		Fee:       math.Round(fee*100) / 100,
		Reason:    "open",
		Timestamp: time.Now(),
		Balance:   p.balance,
	}

	p.balance -= cost + fee
	p.positions[pair] = &PaperPosition{
		Pair:       pair,
		Side:       side,
		EntryPrice: fillPrice,
		Amount:     amount,
		OpenTime:   time.Now(),
		StopLoss:   stopLoss,
		TakeProfit: takeProfit,
	}

	p.trades = append(p.trades, trade)
	log.Info().
		Str("id", trade.ID).
		Str("pair", pair).
		Str("side", string(side)).
		Float64("price", fillPrice).
		Float64("amount", amount).
		Float64("fee", fee).
		Msg("Paper: position opened")

	return &trade, nil
}

// ClosePosition closes a simulated position.
func (p *PaperEngine) ClosePosition(pair string, price float64) (*PaperTrade, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pos, exists := p.positions[pair]
	if !exists {
		return nil, fmt.Errorf("no position on %s", pair)
	}

	// Apply slippage
	fillPrice := price
	if p.slippage > 0 {
		if pos.Side == exchange.Buy {
			fillPrice = price * (1 - p.slippage) // sell to close
		} else {
			fillPrice = price * (1 + p.slippage) // buy to close
		}
	}

	var pnl float64
	if pos.Side == exchange.Buy {
		pnl = (fillPrice - pos.EntryPrice) * pos.Amount
	} else {
		pnl = (pos.EntryPrice - fillPrice) * pos.Amount
	}

	cost := pos.Amount * fillPrice
	fee := cost * p.feeRate

	p.orderIDSeq++
	trade := PaperTrade{
		ID:        fmt.Sprintf("PAPER-%05d", p.orderIDSeq),
		Pair:      pair,
		Side:      pos.Side,
		Price:     fillPrice,
		Amount:    pos.Amount,
		PnL:       math.Round(pnl*100) / 100,
		Fee:       math.Round(fee*100) / 100,
		Reason:    "close",
		Timestamp: time.Now(),
		Balance:   p.balance,
	}

	p.balance += cost - fee
	delete(p.positions, pair)

	p.trades = append(p.trades, trade)
	log.Info().
		Str("id", trade.ID).
		Str("pair", pair).
		Float64("pnl", pnl).
		Float64("balance", p.balance).
		Msg("Paper: position closed")

	return &trade, nil
}

// CheckTPSL checks all positions for TP/SL triggers.
func (p *PaperEngine) CheckTPSL(currentPrices map[string]float64) []PaperTrade {
	results := make([]PaperTrade, 0)
	p.mu.Lock()
	defer p.mu.Unlock()

	for pair, pos := range p.positions {
		price, ok := currentPrices[pair]
		if !ok {
			continue
		}

		triggered := false
		reason := ""

		// Check stop loss
		if pos.StopLoss > 0 {
			if pos.Side == exchange.Buy && price <= pos.StopLoss {
				triggered = true
				reason = "stop_loss"
			} else if pos.Side == exchange.Sell && price >= pos.StopLoss {
				triggered = true
				reason = "stop_loss"
			}
		}

		// Check take profit
		if !triggered && pos.TakeProfit > 0 {
			if pos.Side == exchange.Buy && price >= pos.TakeProfit {
				triggered = true
				reason = "take_profit"
			} else if pos.Side == exchange.Sell && price <= pos.TakeProfit {
				triggered = true
				reason = "take_profit"
			}
		}

		if triggered {
			// Inline close to avoid deadlock (already holding lock)
			fillPrice := price
			var pnl float64
			if pos.Side == exchange.Buy {
				pnl = (fillPrice - pos.EntryPrice) * pos.Amount
			} else {
				pnl = (pos.EntryPrice - fillPrice) * pos.Amount
			}
			cost := pos.Amount * fillPrice
			fee := cost * p.feeRate

			p.orderIDSeq++
			trade := PaperTrade{
				ID:        fmt.Sprintf("PAPER-%05d", p.orderIDSeq),
				Pair:      pair,
				Side:      pos.Side,
				Price:     fillPrice,
				Amount:    pos.Amount,
				PnL:       math.Round(pnl*100) / 100,
				Fee:       math.Round(fee*100) / 100,
				Reason:    reason,
				Timestamp: time.Now(),
				Balance:   p.balance,
			}

			p.balance += cost - fee
			delete(p.positions, pair)
			p.trades = append(p.trades, trade)
			results = append(results, trade)
		}
	}
	return results
}

// GetPositions returns all open positions.
func (p *PaperEngine) GetPositions() map[string]*PaperPosition {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]*PaperPosition, len(p.positions))
	for k, v := range p.positions {
		copy := *v
		result[k] = &copy
	}
	return result
}

// GetTrades returns all trade history.
func (p *PaperEngine) GetTrades() []PaperTrade {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]PaperTrade{}, p.trades...)
}

// GetBalance returns current balance.
func (p *PaperEngine) GetBalance() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.balance
}

// GetEquity returns balance + unrealized PnL.
func (p *PaperEngine) GetEquity() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	equity := p.balance
	for pair, pos := range p.positions {
		ticker, ok := p.tickerCache[pair]
		if !ok {
			continue
		}
		if pos.Side == exchange.Buy {
			equity += pos.Amount * ticker.Last
		} else {
			equity += pos.Amount * (2*pos.EntryPrice - ticker.Last)
		}
	}
	return equity
}

// Summary returns a summary of the paper account.
func (p *PaperEngine) Summary() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	wins := 0
	losses := 0
	totalPnL := 0.0
	for _, t := range p.trades {
		if t.Reason == "close" || t.Reason == "stop_loss" || t.Reason == "take_profit" {
			totalPnL += t.PnL
			if t.PnL > 0 {
				wins++
			} else if t.PnL < 0 {
				losses++
			}
		}
	}

	winRate := 0.0
	if wins+losses > 0 {
		winRate = float64(wins) / float64(wins+losses) * 100
	}

	equity := p.balance
	for pair, pos := range p.positions {
		if ticker, ok := p.tickerCache[pair]; ok {
			if pos.Side == exchange.Buy {
				equity += pos.Amount * ticker.Last
			} else {
				equity += pos.Amount * (2*pos.EntryPrice - ticker.Last)
			}
		}
	}

	return map[string]interface{}{
		"initial_balance": p.initial,
		"current_balance": math.Round(p.balance*100) / 100,
		"equity":          math.Round(equity*100) / 100,
		"total_pnl":       math.Round(totalPnL*100) / 100,
		"return_pct":      math.Round((equity/p.initial-1)*10000) / 100,
		"total_trades":    wins + losses,
		"win_trades":      wins,
		"loss_trades":     losses,
		"win_rate":        math.Round(winRate*100) / 100,
		"open_positions":  len(p.positions),
		"total_fees":      p.totalFees(),
	}
}

func (p *PaperEngine) totalFees() float64 {
	total := 0.0
	for _, t := range p.trades {
		total += t.Fee
	}
	return math.Round(total*100) / 100
}
