package rsi

import (
	"fmt"

	"github.com/bengalm/mkk/pkg/eventbus"
	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/bengalm/mkk/pkg/indicator"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/rs/zerolog/log"
)

func init() {
	strategy.Register("rsi", func() strategy.Strategy { return &RSIStrategy{} })
}

// RSIStrategy trades based on RSI overbought/oversold signals.
type RSIStrategy struct {
	strategy.BaseStrategy
	pair       string
	timeframe  string
	period     int
	overbought float64
	oversold   float64
	takeProfit float64
	stopLoss   float64
	longShort  bool // true = both directions, false = long only
	amount     float64

	candles    []exchange.Candle
	rsiValues  []float64
	position   *openPosition
}

type openPosition struct {
	Side       exchange.OrderSide
	EntryPrice float64
	Amount     float64
}

// Name returns strategy name.
func (r *RSIStrategy) Name() string { return "rsi" }

// Init initializes the RSI strategy.
func (r *RSIStrategy) Init(config map[string]interface{}, ex exchange.Exchange, bus *eventbus.EventBus) error {
	r.InitBase("rsi", ex, bus)

	r.pair = strategy.GetStringParam(config, "pair", "BTC-USDT")
	r.timeframe = strategy.GetStringParam(config, "timeframe", "1h")
	r.period = strategy.GetIntParam(config, "period", 14)
	r.overbought = strategy.GetFloatParam(config, "overbought", 70)
	r.oversold = strategy.GetFloatParam(config, "oversold", 30)
	r.takeProfit = strategy.GetFloatParam(config, "take_profit", 0.05)
	r.stopLoss = strategy.GetFloatParam(config, "stop_loss", 0.03)
	r.longShort = strategy.GetFloatParam(config, "long_short", 1) > 0
	r.amount = strategy.GetFloatParam(config, "amount", 0.01)
	r.candles = make([]exchange.Candle, 0)

	log.Info().
		Str("pair", r.pair).
		Int("period", r.period).
		Float64("overbought", r.overbought).
		Float64("oversold", r.oversold).
		Msg("RSI strategy initialized")

	// Load historical candles for initial RSI calculation
	candles, err := ex.GetCandles(exchange.CandleRequest{
		Pair:      r.pair,
		Timeframe: r.timeframe,
		Limit:     r.period + 50,
	})
	if err != nil {
		log.Warn().Err(err).Msg("Failed to load historical candles for RSI")
	} else {
		r.candles = candles
		r.calculateRSI()
		log.Info().Int("candles", len(r.candles)).Msg("Historical candles loaded")
	}

	return nil
}

// calculateRSI computes RSI from current candles.
func (r *RSIStrategy) calculateRSI() {
	if len(r.candles) < r.period+1 {
		return
	}
	closes := make([]float64, len(r.candles))
	for i, c := range r.candles {
		closes[i] = c.Close
	}
	r.rsiValues = indicator.RSI(closes, r.period)
}

// OnTick checks TP/SL for open positions.
func (r *RSIStrategy) OnTick(ticker exchange.Ticker) {
	if r.position == nil {
		return
	}

	pnl := 0.0
	if r.position.Side == exchange.Buy {
		pnl = (ticker.Last - r.position.EntryPrice) / r.position.EntryPrice
	} else {
		pnl = (r.position.EntryPrice - ticker.Last) / r.position.EntryPrice
	}

	// Take profit
	if r.takeProfit > 0 && pnl >= r.takeProfit {
		r.closePosition(ticker.Last, fmt.Sprintf("RSI TP: pnl=%.2f%%", pnl*100))
		return
	}

	// Stop loss
	if r.stopLoss > 0 && pnl <= -r.stopLoss {
		r.closePosition(ticker.Last, fmt.Sprintf("RSI SL: pnl=%.2f%%", pnl*100))
		return
	}
}

// OnCandle processes new candle data.
func (r *RSIStrategy) OnCandle(candle exchange.Candle) {
	if candle.Pair != "" && candle.Pair != r.pair {
		return
	}

	r.candles = append(r.candles, candle)
	// Keep last 200 candles
	if len(r.candles) > 200 {
		r.candles = r.candles[len(r.candles)-200:]
	}

	r.calculateRSI()
	if len(r.rsiValues) < 2 {
		return
	}

	currentRSI := r.rsiValues[len(r.rsiValues)-1]
	prevRSI := r.rsiValues[len(r.rsiValues)-2]

	// Buy signal: RSI crosses above oversold from below
	if r.position == nil && prevRSI < r.oversold && currentRSI >= r.oversold {
		log.Info().
			Float64("rsi", currentRSI).
			Float64("prev_rsi", prevRSI).
			Msg("RSI buy signal")

		r.EmitSignal(strategy.TradeSignal{
			Action:     strategy.ActionBuy,
			Pair:       r.pair,
			Price:      candle.Close,
			Amount:     r.amount,
			Type:       exchange.OrderMarket,
			StopLoss:   candle.Close * (1 - r.stopLoss),
			TakeProfit: candle.Close * (1 + r.takeProfit),
			Reason:     fmt.Sprintf("RSI crossover buy: %.1f->%.1f", prevRSI, currentRSI),
		})

		r.position = &openPosition{
			Side:       exchange.Buy,
			EntryPrice: candle.Close,
			Amount:     r.amount,
		}
		return
	}

	// Sell signal: RSI crosses below overbought from above
	if r.longShort && r.position == nil && prevRSI > r.overbought && currentRSI <= r.overbought {
		log.Info().
			Float64("rsi", currentRSI).
			Float64("prev_rsi", prevRSI).
			Msg("RSI short signal")

		r.EmitSignal(strategy.TradeSignal{
			Action:     strategy.ActionSell,
			Pair:       r.pair,
			Price:      candle.Close,
			Amount:     r.amount,
			Type:       exchange.OrderMarket,
			StopLoss:   candle.Close * (1 + r.stopLoss),
			TakeProfit: candle.Close * (1 - r.takeProfit),
			Reason:     fmt.Sprintf("RSI crossover short: %.1f->%.1f", prevRSI, currentRSI),
		})

		r.position = &openPosition{
			Side:       exchange.Sell,
			EntryPrice: candle.Close,
			Amount:     r.amount,
		}
		return
	}

	// Close long on overbought
	if r.position != nil && r.position.Side == exchange.Buy && prevRSI > r.overbought && currentRSI <= r.overbought {
		r.closePosition(candle.Close, fmt.Sprintf("RSI overbought close: %.1f", currentRSI))
	}

	// Close short on oversold
	if r.position != nil && r.position.Side == exchange.Sell && prevRSI < r.oversold && currentRSI >= r.oversold {
		r.closePosition(candle.Close, fmt.Sprintf("RSI oversold close: %.1f", currentRSI))
	}
}

// OnFill handles order fills.
func (r *RSIStrategy) OnFill(trade exchange.Trade) {}

func (r *RSIStrategy) closePosition(price float64, reason string) {
	if r.position == nil {
		return
	}

	side := strategy.ActionCloseLong
	if r.position.Side == exchange.Sell {
		side = strategy.ActionCloseShort
	}

	r.EmitSignal(strategy.TradeSignal{
		Action:   side,
		Pair:     r.pair,
		Price:    price,
		Amount:   r.position.Amount,
		Type:     exchange.OrderMarket,
		Reason:   reason,
	})

	pnl := 0.0
	if r.position.Side == exchange.Buy {
		pnl = (price - r.position.EntryPrice) / r.position.EntryPrice * 100
	} else {
		pnl = (r.position.EntryPrice - price) / r.position.EntryPrice * 100
	}

	log.Info().
		Str("side", string(r.position.Side)).
		Float64("entry", r.position.EntryPrice).
		Float64("exit", price).
		Float64("pnl_pct", pnl).
		Msg("Position closed")

	r.position = nil
}

// Stats returns RSI strategy statistics.
func (r *RSIStrategy) Stats() map[string]interface{} {
	currentRSI := 0.0
	if len(r.rsiValues) > 0 {
		currentRSI = r.rsiValues[len(r.rsiValues)-1]
	}
	return map[string]interface{}{
		"pair":        r.pair,
		"period":      r.period,
		"current_rsi": currentRSI,
		"has_position": r.position != nil,
		"candles":     len(r.candles),
	}
}
