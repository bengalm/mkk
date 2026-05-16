package trader

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/bengalm/mkk/pkg/eventbus"
	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/bengalm/mkk/pkg/exchange/okx"
	"github.com/bengalm/mkk/pkg/repository"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/rs/zerolog/log"
)

// RiskConfig holds risk management parameters.
type RiskConfig struct {
	MaxPositionSize  float64 `yaml:"max_position_size"`
	MaxDailyLoss     float64 `yaml:"max_daily_loss"`
	MaxDrawdownPct   float64 `yaml:"max_drawdown_pct"`
	MaxOpenPositions int     `yaml:"max_open_positions"`
	DefaultLeverage  int     `yaml:"default_leverage"`
	MinRiskReward    float64 `yaml:"min_risk_reward"`
}

// DefaultRiskConfig returns sensible defaults.
func DefaultRiskConfig() RiskConfig {
	return RiskConfig{
		MaxPositionSize:  100,
		MaxDailyLoss:     30,
		MaxDrawdownPct:   15,
		MaxOpenPositions: 3,
		DefaultLeverage:  5,
		MinRiskReward:    2.0,
	}
}

// Notifier is the interface for sending notifications.
type Notifier interface {
	NotifyTrade(pair, side string, price, amount float64) error
	NotifyPnL(pair string, pnl float64) error
	NotifyError(context string, errMsg string) error
	NotifyRiskAlert(message string) error
}

// Engine is the live trading engine.
type Engine struct {
	exchange  exchange.Exchange
	bus       *eventbus.EventBus
	manager   *strategy.Manager
	risk      RiskConfig
	repo      *repository.Repository
	notifier  Notifier
	positions map[string]*ManagedPosition
	mu        sync.RWMutex
	running   bool
	quit      chan struct{}

	// Daily P&L tracking
	dailyPnL      float64
	dailyReset    time.Time
	peakEquity    float64
	initialEquity float64
}

// ManagedPosition tracks a position managed by the engine.
type ManagedPosition struct {
	Strategy   string             `json:"strategy"`
	Pair       string             `json:"pair"`
	Side       exchange.OrderSide `json:"side"`
	EntryPrice float64            `json:"entry_price"`
	Amount     float64            `json:"amount"`
	StopLoss   float64            `json:"stop_loss"`
	TakeProfit float64            `json:"take_profit"`
	OpenTime   time.Time          `json:"open_time"`
	OrderID    string             `json:"order_id"`
}

// NewEngine creates a new trading engine.
func NewEngine(ex exchange.Exchange, bus *eventbus.EventBus, risk RiskConfig, repo *repository.Repository, n Notifier) *Engine {
	if n == nil {
		n = noopNotifier{}
	}
	return &Engine{
		exchange:      ex,
		bus:           bus,
		manager:       strategy.NewManager(),
		risk:          risk,
		repo:          repo,
		notifier:      n,
		positions:     make(map[string]*ManagedPosition),
		quit:          make(chan struct{}),
		dailyReset:    time.Now().Truncate(24 * time.Hour),
		initialEquity: 0,
		peakEquity:    0,
	}
}

// noopNotifier is used when no notifier is configured.
type noopNotifier struct{}

func (noopNotifier) NotifyTrade(string, string, float64, float64) error { return nil }
func (noopNotifier) NotifyPnL(string, float64) error                    { return nil }
func (noopNotifier) NotifyError(string, string) error                   { return nil }
func (noopNotifier) NotifyRiskAlert(string) error                       { return nil }

// AddStrategy adds a strategy to the engine.
func (e *Engine) AddStrategy(s strategy.Strategy, config map[string]interface{}) error {
	if err := s.Init(config, e.exchange, e.bus); err != nil {
		return fmt.Errorf("init strategy %s: %w", s.Name(), err)
	}
	e.manager.Add(s)

	go e.consumeSignals(s)

	log.Info().Str("strategy", s.Name()).Msg("Strategy added to trading engine")
	return nil
}

// Start starts the trading engine.
func (e *Engine) Start() error {
	// Restore positions from repository
	if e.repo != nil {
		positions, err := e.repo.GetOpenPositions()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to restore positions from repository")
		} else {
			for _, p := range positions {
				pair, _ := p["pair"].(string)
				side, _ := p["side"].(string)
				strategy, _ := p["strategy"].(string)
				entryPrice, _ := p["entry_price"].(float64)
				amount, _ := p["amount"].(float64)
				stopLoss, _ := p["stop_loss"].(float64)
				takeProfit, _ := p["take_profit"].(float64)
				orderID, _ := p["order_id"].(string)
				mp := &ManagedPosition{
					Strategy:   strategy,
					Pair:       pair,
					Side:       exchange.OrderSide(side),
					EntryPrice: entryPrice,
					Amount:     amount,
					StopLoss:   stopLoss,
					TakeProfit: takeProfit,
					OrderID:    orderID,
				}
				e.positions[pair] = mp
			}
			if len(e.positions) > 0 {
				log.Info().Int("count", len(e.positions)).Msg("Restored positions from repository")
			}
		}
	}

	// Get initial balance
	balances, err := e.exchange.GetBalance("USDT")
	if err != nil {
		return fmt.Errorf("get balance: %w", err)
	}
	for _, b := range balances {
		if b.Currency == "USDT" {
			e.initialEquity = b.Total
			e.peakEquity = b.Total
			break
		}
	}

	e.running = true
	log.Info().
		Float64("equity", e.initialEquity).
		Msg("Trading engine started")

	go e.run()

	return nil
}

// Stop gracefully shuts down the engine.
func (e *Engine) Stop() {
	e.running = false
	close(e.quit)
	e.manager.StopAll()
	log.Info().Msg("Trading engine stopped")
}

// run is the main event loop.
func (e *Engine) run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.checkRiskLimits()
			e.resetDailyPnL()
		case <-e.quit:
			return
		}
	}
}

// signalProvider is an interface for strategies that expose a signal channel.
type signalProvider interface {
	Signals() <-chan strategy.TradeSignal
}

// consumeSignals processes strategy signals.
func (e *Engine) consumeSignals(s strategy.Strategy) {
	sp, ok := s.(signalProvider)
	if !ok {
		return
	}

	for sig := range sp.Signals() {
		if !e.running {
			return
		}
		e.processSignal(sig)
	}
}

// processSignal handles a single trade signal.
func (e *Engine) processSignal(sig strategy.TradeSignal) {
	log.Info().
		Str("action", string(sig.Action)).
		Str("pair", sig.Pair).
		Float64("price", sig.Price).
		Float64("amount", sig.Amount).
		Str("strategy", sig.Strategy).
		Msg("Processing signal")

	if !e.checkPreTrade(sig) {
		log.Warn().Str("pair", sig.Pair).Msg("Signal rejected by risk check")
		e.notifier.NotifyRiskAlert(fmt.Sprintf("Signal rejected for %s: risk check failed (%s)", sig.Pair, sig.Strategy))
		return
	}

	switch sig.Action {
	case strategy.ActionBuy, strategy.ActionSell:
		e.openPosition(sig)
	case strategy.ActionCloseLong, strategy.ActionCloseShort:
		e.closePositionFromSignal(sig)
	default:
		log.Debug().Str("action", string(sig.Action)).Msg("Ignoring signal action")
	}
}

// checkPreTrade validates a signal against risk rules.
func (e *Engine) checkPreTrade(sig strategy.TradeSignal) bool {
	if e.dailyPnL <= -e.risk.MaxDailyLoss {
		log.Warn().
			Float64("daily_loss", e.dailyPnL).
			Float64("max_loss", e.risk.MaxDailyLoss).
			Msg("Daily loss limit reached")
		return false
	}

	e.mu.RLock()
	openCount := len(e.positions)
	e.mu.RUnlock()

	if sig.Action == strategy.ActionBuy || sig.Action == strategy.ActionSell {
		if openCount >= e.risk.MaxOpenPositions {
			log.Warn().
				Int("open", openCount).
				Int("max", e.risk.MaxOpenPositions).
				Msg("Max open positions reached")
			return false
		}
	}

	cost := sig.Amount * sig.Price
	if cost > e.risk.MaxPositionSize {
		log.Warn().
			Float64("cost", cost).
			Float64("max", e.risk.MaxPositionSize).
			Msg("Position size exceeds limit")
		return false
	}

	if sig.StopLoss > 0 && sig.TakeProfit > 0 {
		risk := math.Abs(sig.Price - sig.StopLoss)
		reward := math.Abs(sig.TakeProfit - sig.Price)
		if risk > 0 {
			rr := reward / risk
			if rr < e.risk.MinRiskReward {
				log.Warn().
					Float64("rr", rr).
					Float64("min_rr", e.risk.MinRiskReward).
					Msg("Risk/reward ratio too low")
				return false
			}
		}
	}

	return true
}

// openPosition places a real order.
func (e *Engine) openPosition(sig strategy.TradeSignal) {
	side := exchange.Buy
	if sig.Action == strategy.ActionSell {
		side = exchange.Sell
	}

	if okxEx, ok := e.exchange.(*okx.OKXExchange); ok {
		if err := okxEx.SetLeverage(sig.Pair, e.risk.DefaultLeverage, "isolated"); err != nil {
			log.Warn().Err(err).Str("pair", sig.Pair).Msg("Failed to set leverage (may already be set)")
		}
	}

	orderReq := exchange.OrderRequest{
		Pair:   sig.Pair,
		Side:   side,
		Type:   sig.Type,
		Amount: sig.Amount,
	}

	if sig.Type == exchange.OrderLimit {
		orderReq.Price = sig.Price
	}

	order, err := e.exchange.PlaceOrder(orderReq)
	if err != nil {
		log.Error().Err(err).Str("pair", sig.Pair).Msg("Failed to place order")
		if e.bus != nil {
			e.bus.Publish(eventbus.TopicError, map[string]interface{}{
				"error":  err.Error(),
				"signal": sig,
			})
		}
		e.notifier.NotifyError("place_order", fmt.Sprintf("Failed to place order for %s: %v", sig.Pair, err))
		return
	}

	pos := &ManagedPosition{
		Strategy:   sig.Strategy,
		Pair:       sig.Pair,
		Side:       side,
		EntryPrice: sig.Price,
		Amount:     sig.Amount,
		StopLoss:   sig.StopLoss,
		TakeProfit: sig.TakeProfit,
		OpenTime:   time.Now(),
		OrderID:    order.ID,
	}

	e.mu.Lock()
	e.positions[sig.Pair] = pos
	e.mu.Unlock()

	log.Info().
		Str("pair", sig.Pair).
		Str("side", string(side)).
		Str("order_id", order.ID).
		Float64("price", sig.Price).
		Msg("Position opened")

	if e.bus != nil {
		e.bus.Publish(eventbus.TopicPosition, pos)
	}

	// Send notification
	e.notifier.NotifyTrade(sig.Pair, string(side), sig.Price, sig.Amount)
}

// closePositionFromSignal closes a position based on signal.
func (e *Engine) closePositionFromSignal(sig strategy.TradeSignal) {
	e.mu.Lock()
	pos, exists := e.positions[sig.Pair]
	if !exists {
		e.mu.Unlock()
		log.Warn().Str("pair", sig.Pair).Msg("No position to close")
		return
	}
	delete(e.positions, sig.Pair)
	e.mu.Unlock()

	closeSide := exchange.Sell
	if pos.Side == exchange.Sell {
		closeSide = exchange.Buy
	}

	_, err := e.exchange.PlaceOrder(exchange.OrderRequest{
		Pair:       sig.Pair,
		Side:       closeSide,
		Type:       exchange.OrderMarket,
		Amount:     pos.Amount,
		ReduceOnly: true,
	})
	if err != nil {
		log.Error().Err(err).Str("pair", sig.Pair).Msg("Failed to close position")
		e.notifier.NotifyError("close_position", fmt.Sprintf("Failed to close %s: %v", sig.Pair, err))
		return
	}

	pnl := 0.0
	if pos.Side == exchange.Buy {
		pnl = (sig.Price - pos.EntryPrice) * pos.Amount
	} else {
		pnl = (pos.EntryPrice - sig.Price) * pos.Amount
	}

	e.dailyPnL += pnl

	// Delete position from repository
	if e.repo != nil {
		if err := e.repo.DeletePosition(pos.Pair); err != nil {
			log.Error().Err(err).Str("pair", pos.Pair).Msg("Failed to delete position from repository")
		}
	}

	log.Info().
		Str("pair", sig.Pair).
		Float64("pnl", pnl).
		Float64("daily_pnl", e.dailyPnL).
		Msg("Position closed")

	if e.bus != nil {
		e.bus.Publish(eventbus.TopicTrade, map[string]interface{}{
			"pair":  sig.Pair,
			"pnl":   pnl,
			"price": sig.Price,
		})
	}

	// Send notification
	e.notifier.NotifyPnL(sig.Pair, pnl)
}

// checkRiskLimits monitors drawdown.
func (e *Engine) checkRiskLimits() {
	balances, err := e.exchange.GetBalance("USDT")
	if err != nil {
		return
	}

	equity := 0.0
	for _, b := range balances {
		if b.Currency == "USDT" {
			equity = b.Total
			break
		}
	}

	if equity > e.peakEquity {
		e.peakEquity = equity
	}

	drawdown := 0.0
	if e.peakEquity > 0 {
		drawdown = (e.peakEquity - equity) / e.peakEquity * 100
	}

	if drawdown > e.risk.MaxDrawdownPct {
		log.Error().
			Float64("drawdown", drawdown).
			Float64("max", e.risk.MaxDrawdownPct).
			Msg("Max drawdown exceeded! Stopping engine")

		e.notifier.NotifyRiskAlert(fmt.Sprintf("Max drawdown exceeded: %.2f%% (limit: %.2f%%)", drawdown, e.risk.MaxDrawdownPct))
		e.Stop()
	}
}

// resetDailyPnL resets daily PnL at midnight.
func (e *Engine) resetDailyPnL() {
	now := time.Now().Truncate(24 * time.Hour)
	if now.After(e.dailyReset) {
		log.Info().Float64("yesterday_pnl", e.dailyPnL).Msg("Daily PnL reset")
		e.dailyPnL = 0
		e.dailyReset = now
	}
}

// GetPositions returns current positions.
func (e *Engine) GetPositions() map[string]*ManagedPosition {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make(map[string]*ManagedPosition, len(e.positions))
	for k, v := range e.positions {
		copy := *v
		result[k] = &copy
	}
	return result
}

// GetStats returns engine statistics.
func (e *Engine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"running":        e.running,
		"open_positions": len(e.positions),
		"daily_pnl":      math.Round(e.dailyPnL*100) / 100,
		"peak_equity":    e.peakEquity,
		"initial_equity": e.initialEquity,
		"strategies":     e.manager.List(),
	}
}
