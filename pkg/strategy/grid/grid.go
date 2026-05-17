package grid

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bengalm/mkk/pkg/eventbus"
	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/bengalm/mkk/pkg/exchange/okx"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/rs/zerolog/log"
)

func init() {
	strategy.Register("grid", func() strategy.Strategy { return &GridStrategy{} })
}

type Direction string

const (
	DirAuto  Direction = "auto"
	DirBoth  Direction = "both"
	DirLong  Direction = "long"
	DirShort Direction = "short"
)

type Trend string

const (
	TrendUp   Trend = "up"
	TrendDown Trend = "down"
	TrendSide Trend = "sideways"
)

// AISignal is the AI trend signal file format.
type AISignal struct {
	Direction   string  `json:"direction"`    // "long", "short", "neutral"
	Confidence  float64 `json:"confidence"`   // 0.0 - 1.0
	Reason      string  `json:"reason"`       // human-readable analysis
	Timestamp   string  `json:"timestamp"`    // ISO 8601
	PriceTarget float64 `json:"price_target"` // optional price target
	StopHint    float64 `json:"stop_hint"`    // optional stop-loss hint
}

// GridNotification is sent via EventBus for important events.
type GridNotification struct {
	Type      string  `json:"type"`       // "fill_open", "fill_close", "stop_loss", "emergency", "trend_change", "grid_rebuild", "volatility_pause", "volatility_resume"
	Pair      string  `json:"pair"`
	Price     float64 `json:"price"`
	Side      string  `json:"side"`
	Amount    float64 `json:"amount"`
	PnL       float64 `json:"pnl,omitempty"`
	Reason    string  `json:"reason,omitempty"`
	Direction string  `json:"direction,omitempty"`
	Trend     string  `json:"trend,omitempty"`
}

// GridStrategy — geometric grid with AI trend, risk management, notifications.
type GridStrategy struct {
	strategy.BaseStrategy
	pair            string
	highPrice       float64
	lowPrice        float64
	gridLevels      int
	quantityPerGrid float64
	direction       Direction
	effectiveDir    Direction
	leverage        int
	isSwap          bool
	orders          map[float64]string // price -> orderID
	activeOrders    map[string]bool    // orderID -> active
	profit          float64
	totalTrades     int

	// Geometric spacing
	spacingPct float64
	gridPrices []float64

	// Trend detection (EMA fallback)
	autoTrend    bool
	trendTF      string
	emaFast      int
	emaSlow      int
	currentTrend Trend
	lastTrendChk time.Time

	// AI signal file
	aiSignalPath string
	lastAISignal *AISignal
	aiMu         sync.RWMutex

	// ATR
	atrPeriod  int
	atrMult    float64
	currentATR float64
	autoRange  bool

	// Auto shift
	autoShift      bool
	shiftThreshold float64
	lastBuildPrice float64
	rebuildCount   int

	// Stop-loss
	stopLossPrice float64
	stopped       bool

	// Drawdown kill switch
	maxDrawdownPct float64
	startEquity   float64

	// Volatility filter
	volatilityFilter bool
	volMultiplier    float64
	avgATR           float64
	paused           bool
	startTime        time.Time
	maxRuntime       time.Duration
}

func (g *GridStrategy) Name() string { return "grid" }

func (g *GridStrategy) Init(config map[string]interface{}, ex exchange.Exchange, bus *eventbus.EventBus) error {
	g.InitBase("grid", ex, bus)

	g.pair = strategy.GetStringParam(config, "pair", "BTC-USDT")
	g.gridLevels = strategy.GetIntParam(config, "grid_levels", 15)
	g.quantityPerGrid = strategy.GetFloatParam(config, "quantity_per_grid", 0.001)
	g.leverage = strategy.GetIntParam(config, "leverage", 3)

	// Direction
	dirStr := strings.ToLower(strategy.GetStringParam(config, "direction", "auto"))
	g.direction = Direction(dirStr)
	g.autoTrend = g.direction == DirAuto
	g.effectiveDir = g.direction
	if g.autoTrend {
		g.effectiveDir = DirBoth
	}

	// Trend params
	g.trendTF = strategy.GetStringParam(config, "trend_timeframe", "4H")
	g.emaFast = strategy.GetIntParam(config, "ema_fast", 20)
	g.emaSlow = strategy.GetIntParam(config, "ema_slow", 50)

	// AI signal file path (default: same dir as binary)
	g.aiSignalPath = strategy.GetStringParam(config, "ai_signal_path", "ai_signal.json")

	// Geometric spacing (%)
	g.spacingPct = strategy.GetFloatParam(config, "spacing_pct", 0)
	if g.spacingPct <= 0 {
		g.spacingPct = 1.5
	}

	// ATR
	g.autoRange = strategy.GetBoolParam(config, "auto_range", true)
	g.atrPeriod = strategy.GetIntParam(config, "atr_period", 14)
	g.atrMult = strategy.GetFloatParam(config, "atr_mult", 3.0)

	// Static range fallback
	g.highPrice = strategy.GetFloatParam(config, "high_price", 0)
	g.lowPrice = strategy.GetFloatParam(config, "low_price", 0)

	// Auto shift
	g.autoShift = strategy.GetBoolParam(config, "auto_shift", true)
	g.shiftThreshold = strategy.GetFloatParam(config, "shift_threshold", 2.0)

	// Stop-loss & drawdown
	g.maxDrawdownPct = strategy.GetFloatParam(config, "max_drawdown_pct", 0.10)

	// Volatility filter
	g.volatilityFilter = strategy.GetBoolParam(config, "volatility_filter", true)
	g.volMultiplier = strategy.GetFloatParam(config, "vol_multiplier", 2.5)

	// Max runtime
	hours := strategy.GetFloatParam(config, "max_runtime_hours", 168)
	g.maxRuntime = time.Duration(hours * float64(time.Hour))

	// Detect swap
	g.isSwap = strings.Contains(strings.ToUpper(g.pair), "-SWAP") ||
		strings.Contains(strings.ToUpper(g.pair), "-FUTURES")

	g.orders = make(map[float64]string)
	g.activeOrders = make(map[string]bool)
	g.gridPrices = make([]float64, 0)
	g.startTime = time.Now()

	// Set leverage
	if g.isSwap {
		if okxEx, ok := g.Exchange().(*okx.OKXExchange); ok {
			if err := okxEx.SetLeverage(g.pair, g.leverage, "isolated"); err != nil {
				log.Warn().Err(err).Str("pair", g.pair).Msg("Failed to set leverage")
			}
		}
	}

	// Get starting equity
	g.startEquity = g.getEquity()

	// Calculate ATR & range
	if err := g.calculateATR(); err != nil {
		return fmt.Errorf("calculate ATR: %w", err)
	}
	if err := g.calculateRange(); err != nil {
		return fmt.Errorf("calculate range: %w", err)
	}

	// Stop-loss
	g.stopLossPrice = math.Round((g.lowPrice-g.currentATR*1.5)*100) / 100

	// Load AI signal if available, otherwise detect trend via EMA
	if g.autoTrend {
		if err := g.loadAISignal(); err != nil {
			log.Debug().Err(err).Msg("No AI signal file, using EMA trend detection")
			if err := g.detectTrend(); err != nil {
				log.Warn().Err(err).Msg("Initial trend detection failed")
			}
		}
	}

	g.calculateGridPrices()

	g.notify("grid_init", GridNotification{
		Type:      "grid_init",
		Pair:      g.pair,
		Direction: string(g.effectiveDir),
		Trend:     string(g.currentTrend),
		Reason:    fmt.Sprintf("Grid started: %s %s, levels=%d, spacing=%.1f%%, ATR=%.2f, SL=%.2f", g.effectiveDir, g.pair, g.gridLevels, g.spacingPct, g.currentATR, g.stopLossPrice),
	})

	log.Info().
		Str("pair", g.pair).
		Float64("high", g.highPrice).
		Float64("low", g.lowPrice).
		Float64("stop_loss", g.stopLossPrice).
		Int("levels", g.gridLevels).
		Float64("spacing_pct", g.spacingPct).
		Str("direction", string(g.direction)).
		Str("effective_dir", string(g.effectiveDir)).
		Str("trend", string(g.currentTrend)).
		Float64("atr", g.currentATR).
		Float64("start_equity", g.startEquity).
		Float64("max_drawdown_pct", g.maxDrawdownPct*100).
		Bool("volatility_filter", g.volatilityFilter).
		Str("ai_signal_path", g.aiSignalPath).
		Msg("Grid strategy initialized")

	return g.placeGridOrders()
}

// ── AI Signal File ──

func (g *GridStrategy) loadAISignal() error {
	g.aiMu.Lock()
	defer g.aiMu.Unlock()

	data, err := os.ReadFile(g.aiSignalPath)
	if err != nil {
		return err
	}

	var sig AISignal
	if err := json.Unmarshal(data, &sig); err != nil {
		return err
	}

	// Validate timestamp (must be within 12 hours)
	ts, err := time.Parse(time.RFC3339, sig.Timestamp)
	if err == nil && time.Since(ts) > 12*time.Hour {
		return fmt.Errorf("AI signal too old: %s", sig.Timestamp)
	}

	prevDir := g.effectiveDir
	g.lastAISignal = &sig

	// Apply AI direction if confidence >= 0.6
	if sig.Confidence >= 0.6 {
		switch strings.ToLower(sig.Direction) {
		case "long", "bullish", "buy":
			g.currentTrend = TrendUp
			g.effectiveDir = DirLong
		case "short", "bearish", "sell":
			g.currentTrend = TrendDown
			g.effectiveDir = DirShort
		default:
			g.currentTrend = TrendSide
			g.effectiveDir = DirBoth
		}
	} else {
		// Low confidence → sideways / both
		g.currentTrend = TrendSide
		g.effectiveDir = DirBoth
	}

	log.Info().
		Str("ai_direction", sig.Direction).
		Float64("confidence", sig.Confidence).
		Str("reason", sig.Reason).
		Str("effective_dir", string(g.effectiveDir)).
		Msg("AI signal loaded")

	// Rebuild grid if direction changed
	if prevDir != "" && prevDir != g.effectiveDir {
		g.notify("trend_change", GridNotification{
			Type:      "trend_change",
			Pair:      g.pair,
			Direction: string(g.effectiveDir),
			Trend:     string(g.currentTrend),
			Reason:    fmt.Sprintf("AI趋势变化: %s→%s (%s)", prevDir, g.effectiveDir, sig.Reason),
		})
		return g.rebuildGrid("ai_signal_change")
	}

	return nil
}

func (g *GridStrategy) saveAISignal(dir, reason string, confidence float64) error {
	sig := AISignal{
		Direction:  dir,
		Confidence: confidence,
		Reason:     reason,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(sig, "", "  ")
	if err != nil {
		return err
	}
	// Ensure directory exists
	dirPath := filepath.Dir(g.aiSignalPath)
	if dirPath != "." && dirPath != "" {
		os.MkdirAll(dirPath, 0755)
	}
	return os.WriteFile(g.aiSignalPath, data, 0644)
}

// ── Notification via EventBus ──

func (g *GridStrategy) notify(notifType string, data GridNotification) {
	if g.GetBus() != nil {
		g.GetBus().Publish(eventbus.TopicStrategy, data)
	}
	log.Info().
		Str("notif_type", notifType).
		Str("pair", data.Pair).
		Str("reason", data.Reason).
		Msg("Grid notification sent")
}

// ── Core Methods ──

func (g *GridStrategy) calculateATR() error {
	candles, err := g.Exchange().GetCandles(exchange.CandleRequest{
		Pair:      g.pair,
		Timeframe: g.trendTF,
		Limit:     g.atrPeriod + 30,
	})
	if err != nil {
		return err
	}

	g.currentATR = calcATR(candles, g.atrPeriod)

	if len(candles) > g.atrPeriod+20 {
		g.avgATR = calcATR(candles[len(candles)-g.atrPeriod-20:], g.atrPeriod)
	} else {
		g.avgATR = g.currentATR
	}

	return nil
}

func (g *GridStrategy) calculateRange() error {
	ticker, err := g.Exchange().GetTicker(g.pair)
	if err != nil {
		return err
	}

	if g.autoRange && g.currentATR > 0 {
		halfRange := g.currentATR * g.atrMult
		g.highPrice = math.Round((ticker.Last+halfRange)*100) / 100
		g.lowPrice = math.Round((ticker.Last-halfRange)*100) / 100
	} else if g.highPrice == 0 || g.lowPrice == 0 {
		g.highPrice = math.Round(ticker.Last*1.06*100) / 100
		g.lowPrice = math.Round(ticker.Last*0.94*100) / 100
	}

	g.lastBuildPrice = ticker.Last
	return nil
}

func (g *GridStrategy) calculateGridPrices() {
	g.gridPrices = make([]float64, 0, g.gridLevels)
	ratio := 1 + g.spacingPct/100

	price := g.lowPrice
	for i := 0; i < g.gridLevels; i++ {
		p := math.Round(price*100) / 100
		if p <= g.highPrice {
			g.gridPrices = append(g.gridPrices, p)
		}
		price = price * ratio
	}

	log.Info().
		Int("levels", len(g.gridPrices)).
		Float64("spacing_pct", g.spacingPct).
		Float64("lowest", g.gridPrices[0]).
		Float64("highest", g.gridPrices[len(g.gridPrices)-1]).
		Msg("Geometric grid prices calculated")
}

func (g *GridStrategy) detectTrend() error {
	candles, err := g.Exchange().GetCandles(exchange.CandleRequest{
		Pair:      g.pair,
		Timeframe: g.trendTF,
		Limit:     g.emaSlow + 10,
	})
	if err != nil {
		return err
	}
	if len(candles) < g.emaSlow {
		return fmt.Errorf("not enough candles: %d", len(candles))
	}

	closes := make([]float64, len(candles))
	for i, c := range candles {
		closes[i] = c.Close
	}

	emaF := calcEMA(closes, g.emaFast)
	emaS := calcEMA(closes, g.emaSlow)

	prevTrend := g.currentTrend
	threshold := 0.003
	ratio := (emaF - emaS) / emaS

	if ratio > threshold {
		g.currentTrend = TrendUp
		g.effectiveDir = DirLong
	} else if ratio < -threshold {
		g.currentTrend = TrendDown
		g.effectiveDir = DirShort
	} else {
		g.currentTrend = TrendSide
		g.effectiveDir = DirBoth
	}

	g.lastTrendChk = time.Now()

	if prevTrend != "" && prevTrend != g.currentTrend {
		g.notify("trend_change", GridNotification{
			Type:      "trend_change",
			Pair:      g.pair,
			Trend:     string(g.currentTrend),
			Direction: string(g.effectiveDir),
			Reason:    fmt.Sprintf("EMA趋势变化: %s→%s (ema20=%.2f, ema50=%.2f)", prevTrend, g.currentTrend, emaF, emaS),
		})
		return g.rebuildGrid("trend_change")
	}

	log.Info().
		Float64("ema_fast", emaF).
		Float64("ema_slow", emaS).
		Str("trend", string(g.currentTrend)).
		Msg("Trend check done")
	return nil
}

func (g *GridStrategy) placeGridOrders() error {
	ticker, err := g.Exchange().GetTicker(g.pair)
	if err != nil {
		return err
	}

	log.Info().
		Float64("price", ticker.Last).
		Str("direction", string(g.effectiveDir)).
		Int("grid_levels", len(g.gridPrices)).
		Msg("Placing geometric grid orders")

	placed := 0
	for _, price := range g.gridPrices {
		switch g.effectiveDir {
		case DirLong:
			if price < ticker.Last {
				g.placeOrderAt(price, exchange.Buy, false)
				placed++
			}
		case DirShort:
			if price > ticker.Last {
				g.placeOrderAt(price, exchange.Sell, false)
				placed++
			}
		case DirBoth:
			if price >= ticker.Last {
				g.placeOrderAt(price, exchange.Sell, false)
			} else {
				g.placeOrderAt(price, exchange.Buy, false)
			}
			placed++
		}
	}

	log.Info().Int("placed", placed).Msg("Grid orders placed")
	return nil
}

func (g *GridStrategy) placeOrderAt(price float64, side exchange.OrderSide, reduceOnly bool) {
	req := exchange.OrderRequest{
		Pair:       g.pair,
		Side:       side,
		Type:       exchange.OrderLimit,
		Price:      price,
		Amount:     g.quantityPerGrid,
		ReduceOnly: reduceOnly,
	}
	order, err := g.Exchange().PlaceOrder(req)
	if err != nil {
		log.Error().Float64("price", price).Str("side", string(side)).Bool("ro", reduceOnly).Err(err).Msg("order failed")
		return
	}
	g.orders[price] = order.ID
	g.activeOrders[order.ID] = true
	log.Debug().Float64("price", price).Str("side", string(side)).Str("id", order.ID).Msg("order placed")
}

// OnTick — stop-loss, drawdown, auto-shift, AI signal reload.
func (g *GridStrategy) OnTick(ticker exchange.Ticker) {
	if g.stopped || g.paused {
		return
	}

	// ── AI signal reload check (every 5 min) ──
	if g.autoTrend && g.lastAISignal != nil {
		g.aiMu.RLock()
		lastTS, _ := time.Parse(time.RFC3339, g.lastAISignal.Timestamp)
		g.aiMu.RUnlock()
		if time.Since(lastTS) > 5*time.Minute {
			// Check file mod time
			if info, err := os.Stat(g.aiSignalPath); err == nil {
				if info.ModTime().After(lastTS) {
					if err := g.loadAISignal(); err != nil {
						log.Debug().Err(err).Msg("AI signal reload failed")
					}
				}
			}
		}
	}

	// ── Stop-loss check ──
	if g.stopLossPrice > 0 {
		if g.effectiveDir == DirLong && ticker.Last <= g.stopLossPrice {
			log.Warn().Float64("price", ticker.Last).Float64("stop_loss", g.stopLossPrice).Msg("STOP-LOSS triggered!")
			g.emergencyClose("stop_loss")
			return
		}
		if g.effectiveDir == DirShort && ticker.Last >= g.stopLossPrice {
			log.Warn().Float64("price", ticker.Last).Float64("stop_loss", g.stopLossPrice).Msg("STOP-LOSS triggered!")
			g.emergencyClose("stop_loss")
			return
		}
	}

	// ── Drawdown kill switch ──
	if g.maxDrawdownPct > 0 && g.startEquity > 0 {
		equity := g.getEquity()
		drawdown := (g.startEquity - equity) / g.startEquity
		if drawdown >= g.maxDrawdownPct {
			log.Warn().
				Float64("drawdown_pct", drawdown*100).
				Float64("equity", equity).
				Float64("start", g.startEquity).
				Msg("MAX DRAWDOWN reached!")
			g.emergencyClose("max_drawdown")
			return
		}
	}

	// ── Max runtime ──
	if g.maxRuntime > 0 && time.Since(g.startTime) > g.maxRuntime {
		log.Info().Msg("Max runtime reached, closing grid")
		g.emergencyClose("max_runtime")
		return
	}

	// ── Auto shift ──
	if g.autoShift && g.lastBuildPrice > 0 {
		deviation := math.Abs(ticker.Last-g.lastBuildPrice) / g.lastBuildPrice * 100
		if deviation > g.shiftThreshold {
			log.Info().Float64("deviation_pct", deviation).Msg("Price shifted, rebuilding grid")
			_ = g.rebuildGrid("auto_shift")
		}
	}

	// ── Breakout detection ──
	g.handleBreakout(ticker.Last)
}

// OnCandle — trend check, volatility filter, ATR update.
func (g *GridStrategy) OnCandle(candle exchange.Candle) {
	if g.stopped {
		return
	}
	if candle.Pair != "" && candle.Pair != g.pair {
		return
	}

	if time.Since(g.lastTrendChk) > 30*time.Minute {
		_ = g.calculateATR()

		// Volatility filter
		if g.volatilityFilter && g.avgATR > 0 {
			if g.currentATR > g.avgATR*g.volMultiplier {
				if !g.paused {
					log.Warn().
						Float64("atr", g.currentATR).
						Float64("avg_atr", g.avgATR).
						Msg("Volatility spike, PAUSING grid")
					g.notify("volatility_pause", GridNotification{
						Type:   "volatility_pause",
						Pair:   g.pair,
						Reason: fmt.Sprintf("波动率飙升暂停 (ATR=%.2f, 均值=%.2f)", g.currentATR, g.avgATR),
					})
					g.pauseGrid()
				}
				return
			} else if g.paused {
				log.Info().Msg("Volatility normalized, RESUMING grid")
				g.notify("volatility_resume", GridNotification{
					Type:   "volatility_resume",
					Pair:   g.pair,
					Reason: "波动率恢复正常，网格恢复",
				})
				g.resumeGrid()
			}
		}

		// Trend re-detection (only if no AI signal)
		if g.autoTrend {
			g.aiMu.RLock()
			hasAI := g.lastAISignal != nil
			g.aiMu.RUnlock()
			if !hasAI {
				_ = g.detectTrend()
			}
		}

		g.stopLossPrice = math.Round((g.lowPrice-g.currentATR*1.5)*100) / 100

		// Dynamic spacing adjustment
		g.adjustSpacing()
	}
}

// OnFill handles order fills with grid cycling + notifications.
func (g *GridStrategy) OnFill(trade exchange.Trade) {
	if g.stopped || g.paused {
		return
	}
	if !g.activeOrders[trade.OrderID] {
		return
	}
	delete(g.activeOrders, trade.OrderID)

	var filledPrice float64
	for price, id := range g.orders {
		if id == trade.OrderID {
			filledPrice = price
			delete(g.orders, price)
			break
		}
	}
	if filledPrice == 0 {
		return
	}

	// Determine if this is an opening or closing fill
	isOpen := true
	if g.effectiveDir == DirLong {
		isOpen = trade.Side == exchange.Buy // buy=open, sell=close
	} else if g.effectiveDir == DirShort {
		isOpen = trade.Side == exchange.Sell // sell=open, buy=close
	}

	switch g.effectiveDir {
	case DirLong:
		g.handleLongFill(trade, filledPrice)
	case DirShort:
		g.handleShortFill(trade, filledPrice)
	default:
		g.handleBothFill(trade, filledPrice)
	}

	// Send notification
	notifType := "fill_open"
	side := "开多"
	if !isOpen {
		notifType = "fill_close"
		if g.effectiveDir == DirShort {
			side = "平空"
		} else {
			side = "平多"
		}
	} else {
		if g.effectiveDir == DirShort {
			side = "开空"
		} else {
			side = "开多"
		}
	}

	g.notify(notifType, GridNotification{
		Type:      notifType,
		Pair:      g.pair,
		Price:     filledPrice,
		Side:      side,
		Amount:    g.quantityPerGrid,
		PnL:       math.Round(g.profit*100) / 100,
		Reason:    fmt.Sprintf("网格成交 %s %.4f @ %.2f | 累计利润: %.2f | 交易次数: %d", side, g.quantityPerGrid, filledPrice, g.profit, g.totalTrades),
		Direction: string(g.effectiveDir),
	})

	// Persist state after each fill
	if err := g.SaveState(); err != nil {
		log.Debug().Err(err).Msg("Failed to save grid state after fill")
	}
}

func (g *GridStrategy) findNextLevelUp(price float64) float64 {
	ratio := 1 + g.spacingPct/100
	next := math.Round(price*ratio*100) / 100
	if next > g.highPrice {
		return 0
	}
	return next
}

func (g *GridStrategy) findNextLevelDown(price float64) float64 {
	ratio := 1 + g.spacingPct/100
	prev := math.Round(price/ratio*100) / 100
	if prev < g.lowPrice {
		return 0
	}
	return prev
}

func (g *GridStrategy) handleLongFill(trade exchange.Trade, filledPrice float64) {
	if trade.Side == exchange.Buy {
		sellPrice := g.findNextLevelUp(filledPrice)
		if sellPrice > 0 {
			g.placeOrderAt(sellPrice, exchange.Sell, true)
			g.recordProfit(filledPrice, sellPrice)
			log.Info().
				Float64("buy", filledPrice).
				Float64("sell_close", sellPrice).
				Float64("profit", g.profit).
				Msg("Long: buy filled → sell close placed")
		}
	} else {
		buyPrice := g.findNextLevelDown(filledPrice)
		if buyPrice > 0 {
			g.placeOrderAt(buyPrice, exchange.Buy, false)
			log.Info().
				Float64("sell_close", filledPrice).
				Float64("new_buy", buyPrice).
				Msg("Long: sell filled → new buy placed")
		}
	}
}

func (g *GridStrategy) handleShortFill(trade exchange.Trade, filledPrice float64) {
	if trade.Side == exchange.Sell {
		buyPrice := g.findNextLevelDown(filledPrice)
		if buyPrice > 0 {
			g.placeOrderAt(buyPrice, exchange.Buy, true)
			g.recordProfit(buyPrice, filledPrice)
			log.Info().
				Float64("sell", filledPrice).
				Float64("buy_close", buyPrice).
				Float64("profit", g.profit).
				Msg("Short: sell filled → buy close placed")
		}
	} else {
		sellPrice := g.findNextLevelUp(filledPrice)
		if sellPrice > 0 {
			g.placeOrderAt(sellPrice, exchange.Sell, false)
			log.Info().
				Float64("buy_close", filledPrice).
				Float64("new_sell", sellPrice).
				Msg("Short: buy filled → new sell placed")
		}
	}
}

func (g *GridStrategy) handleBothFill(trade exchange.Trade, filledPrice float64) {
	if trade.Side == exchange.Buy {
		sellPrice := g.findNextLevelUp(filledPrice)
		if sellPrice > 0 {
			g.placeOrderAt(sellPrice, exchange.Sell, false)
			g.recordProfit(filledPrice, sellPrice)
		}
	} else {
		buyPrice := g.findNextLevelDown(filledPrice)
		if buyPrice > 0 {
			g.placeOrderAt(buyPrice, exchange.Buy, false)
			g.recordProfit(buyPrice, filledPrice)
		}
	}
}

func (g *GridStrategy) recordProfit(entryPrice, exitPrice float64) {
	var pnl float64
	if g.effectiveDir == DirShort {
		pnl = (entryPrice - exitPrice) * g.quantityPerGrid
	} else {
		pnl = (exitPrice - entryPrice) * g.quantityPerGrid
	}
	g.profit += pnl
	g.totalTrades++
}

func (g *GridStrategy) pauseGrid() {
	g.paused = true
	for orderID := range g.activeOrders {
		_ = g.Exchange().CancelOrder(g.pair, orderID)
	}
	g.orders = make(map[float64]string)
	g.activeOrders = make(map[string]bool)
	log.Warn().Msg("Grid PAUSED")
}

func (g *GridStrategy) resumeGrid() {
	g.paused = false
	g.orders = make(map[float64]string)
	g.activeOrders = make(map[string]bool)
	_ = g.placeGridOrders()
	log.Info().Msg("Grid RESUMED")
}

func (g *GridStrategy) emergencyClose(reason string) {
	g.stopped = true
	for orderID := range g.activeOrders {
		_ = g.Exchange().CancelOrder(g.pair, orderID)
	}
	g.orders = make(map[float64]string)
	g.activeOrders = make(map[string]bool)

	if g.isSwap {
		g.closeAllPositions()
	}

	g.notify("emergency", GridNotification{
		Type:   "emergency",
		Pair:   g.pair,
		PnL:    math.Round(g.profit*100) / 100,
		Reason: fmt.Sprintf("🚨 紧急平仓: %s | 利润=%.2f | 交易=%d", reason, g.profit, g.totalTrades),
	})

	log.Warn().
		Str("reason", reason).
		Float64("total_profit", g.profit).
		Int("total_trades", g.totalTrades).
		Msg("EMERGENCY CLOSE executed")
}

func (g *GridStrategy) closeAllPositions() {
	positions, err := g.Exchange().GetPositions(g.pair)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get positions for emergency close")
		return
	}
	for _, pos := range positions {
		if pos.Size == 0 {
			continue
		}
		side := exchange.Sell
		if pos.Side == "short" {
			side = exchange.Buy
		}
		_, err := g.Exchange().PlaceOrder(exchange.OrderRequest{
			Pair:       g.pair,
			Side:       side,
			Type:       exchange.OrderMarket,
			Amount:     pos.Size,
			ReduceOnly: true,
		})
		if err != nil {
			log.Error().Err(err).Str("side", string(side)).Msg("Emergency close position failed")
		} else {
			log.Info().Str("side", string(pos.Side)).Float64("size", pos.Size).Msg("Position closed")
		}
	}
}

func (g *GridStrategy) getEquity() float64 {
	bals, err := g.Exchange().GetBalance("USDT")
	if err != nil || len(bals) == 0 {
		return g.startEquity
	}
	return bals[0].Total
}

func (g *GridStrategy) rebuildGrid(reason string) error {
	log.Info().Str("reason", reason).Msg("Rebuilding grid")

	cancelled := 0
	for orderID := range g.activeOrders {
		if err := g.Exchange().CancelOrder(g.pair, orderID); err != nil {
			log.Error().Str("id", orderID).Err(err).Msg("cancel failed")
		} else {
			cancelled++
		}
	}

	g.orders = make(map[float64]string)
	g.activeOrders = make(map[string]bool)

	if g.autoRange {
		_ = g.calculateATR()
		_ = g.calculateRange()
	}

	g.stopLossPrice = math.Round((g.lowPrice-g.currentATR*1.5)*100) / 100
	g.calculateGridPrices()
	g.rebuildCount++

	g.notify("grid_rebuild", GridNotification{
		Type:      "grid_rebuild",
		Pair:      g.pair,
		Direction: string(g.effectiveDir),
		Reason:    fmt.Sprintf("网格重建: %s | 方向=%s | 区间=%.2f-%.2f", reason, g.effectiveDir, g.lowPrice, g.highPrice),
	})

	if err := g.placeGridOrders(); err != nil {
		return err
	}

	log.Info().Int("cancelled", cancelled).Int("rebuilds", g.rebuildCount).Msg("Grid rebuilt")
	return nil
}

func (g *GridStrategy) Stop() {
	for orderID := range g.activeOrders {
		_ = g.Exchange().CancelOrder(g.pair, orderID)
	}
	g.BaseStrategy.Stop()
	log.Info().
		Float64("profit", math.Round(g.profit*100)/100).
		Int("trades", g.totalTrades).
		Int("rebuilds", g.rebuildCount).
		Bool("stopped", g.stopped).
		Msg("Grid stopped")
}

func (g *GridStrategy) Stats() map[string]interface{} {
	g.aiMu.RLock()
	hasAI := g.lastAISignal != nil
	aiDir := ""
	aiConf := 0.0
	if hasAI {
		aiDir = g.lastAISignal.Direction
		aiConf = g.lastAISignal.Confidence
	}
	g.aiMu.RUnlock()

	// Performance metrics
	runtime := time.Since(g.startTime)
	var fillRate, pnlPerTrade, equityReturn float64
	if g.totalTrades > 0 {
		fillRate = float64(g.totalTrades) / runtime.Hours()
		pnlPerTrade = g.profit / float64(g.totalTrades)
	}
	equity := g.getEquity()
	if g.startEquity > 0 {
		equityReturn = (equity - g.startEquity) / g.startEquity * 100
	}

	// Inventory
	inventorySize := 0.0
	inventoryPct := 0.0
	positions, _ := g.Exchange().GetPositions(g.pair)
	for _, p := range positions {
		inventorySize += p.Size
	}
	if equity > 0 {
		// inventoryPct = position notional / equity
		// need current price for notional
		if ticker, err := g.Exchange().GetTicker(g.pair); err == nil {
			inventoryPct = (inventorySize * ticker.Last) / equity * 100
		}
	}

	return map[string]interface{}{
		"pair":            g.pair,
		"high":            g.highPrice,
		"low":             g.lowPrice,
		"stop_loss":       g.stopLossPrice,
		"levels":          len(g.gridPrices),
		"spacing_pct":     g.spacingPct,
		"direction":       string(g.direction),
		"effective_dir":   string(g.effectiveDir),
		"trend":           string(g.currentTrend),
		"atr":             math.Round(g.currentATR*100) / 100,
		"active_orders":   len(g.activeOrders),
		"profit":          math.Round(g.profit*100) / 100,
		"trades":          g.totalTrades,
		"rebuilds":        g.rebuildCount,
		"paused":          g.paused,
		"stopped":         g.stopped,
		"runtime_hours":   math.Round(runtime.Hours()*10) / 10,
		"ai_signal":       hasAI,
		"ai_direction":    aiDir,
		"ai_confidence":   math.Round(aiConf*100) / 100,
		// Performance metrics
		"equity":          math.Round(equity*100) / 100,
		"equity_return":   math.Round(equityReturn*100) / 100,
		"fill_rate_h":     math.Round(fillRate*100) / 100, // fills per hour
		"pnl_per_trade":   math.Round(pnlPerTrade*100) / 100,
		"inventory_size":  math.Round(inventorySize*1000) / 1000,
		"inventory_pct":   math.Round(inventoryPct*10) / 10,
		"start_equity":    math.Round(g.startEquity*100) / 100,
	}
}

// ── Breakout Detection & Handling ──

// handleBreakout detects price breaking out of grid range and responds.
func (g *GridStrategy) handleBreakout(price float64) {
	if g.highPrice == 0 || g.lowPrice == 0 || g.currentATR == 0 {
		return
	}

	breakoutAbove := price > g.highPrice
	breakoutBelow := price < g.lowPrice

	if !breakoutAbove && !breakoutBelow {
		return
	}

	// Calculate how far price is beyond grid range
	var distancePct float64
	if breakoutAbove {
		distancePct = (price - g.highPrice) / g.highPrice * 100
	} else {
		distancePct = (g.lowPrice - price) / g.lowPrice * 100
	}

	// Only act if price is >1% beyond grid range
	if distancePct < 1.0 {
		return
	}

	// Get current position to assess inventory imbalance
	positions, err := g.Exchange().GetPositions(g.pair)
	if err != nil || len(positions) == 0 {
		// No position — just widen grid
		g.handleBreakoutWiden(price, breakoutAbove)
		return
	}

	posSize := 0.0
	for _, p := range positions {
		posSize += p.Size
	}

	// If position is small relative to equity, widen grid
	equity := g.getEquity()
	if equity == 0 {
		return
	}
	positionPct := (posSize * price) / equity

	if positionPct < 0.30 {
		// Low inventory — widen grid
		g.handleBreakoutWiden(price, breakoutAbove)
	} else if positionPct < 0.60 {
		// Medium inventory — widen + hedge partial
		g.handleBreakoutHedge(price, breakoutAbove, positions)
	} else {
		// Heavy inventory — emergency close
		direction := "上方"
		if breakoutBelow {
			direction = "下方"
		}
		g.emergencyClose(fmt.Sprintf("突破%s %.1f%%, 仓位过重 %.0f%%", direction, distancePct, positionPct*100))
	}
}

// handleBreakoutWiden rebuilds grid centered on current price with wider range.
func (g *GridStrategy) handleBreakoutWiden(price float64, above bool) {
	// Expand grid range by 50%
	rangeSize := g.highPrice - g.lowPrice
	expandBy := rangeSize * 0.25 // expand each side by 25%

	if above {
		g.highPrice += expandBy
		g.lowPrice += expandBy * 0.5 // slight upward shift
	} else {
		g.lowPrice -= expandBy
		g.highPrice -= expandBy * 0.5 // slight downward shift
	}

	// Rebuild grid with new bounds
	g.notify("breakout_widen", GridNotification{
		Type:   "breakout_widen",
		Pair:   g.pair,
		Price:  price,
		Reason: fmt.Sprintf("突破! 价格%.2f超出网格, 扩大范围至 %.2f-%.2f", price, g.lowPrice, g.highPrice),
	})
	_ = g.rebuildGrid("breakout_widen")
}

// handleBreakoutHedge widens grid and partially hedges position.
func (g *GridStrategy) handleBreakoutHedge(price float64, above bool, positions []exchange.Position) {
	// First widen
	g.handleBreakoutWiden(price, above)

	// Then hedge 30% of position
	for _, pos := range positions {
		if pos.Size == 0 {
			continue
		}
		hedgeQty := pos.Size * 0.3
		hedgeSide := exchange.Sell
		if pos.Side == "short" {
			hedgeSide = exchange.Buy
		}
		_, err := g.Exchange().PlaceOrder(exchange.OrderRequest{
			Pair:       g.pair,
			Side:       hedgeSide,
			Type:       exchange.OrderMarket,
			Amount:     hedgeQty,
			ReduceOnly: true,
		})
		if err != nil {
			log.Error().Err(err).Msg("Breakout hedge order failed")
		} else {
			g.notify("breakout_hedge", GridNotification{
				Type:   "breakout_hedge",
				Pair:   g.pair,
				Price:  price,
				Reason: fmt.Sprintf("突破对冲: %s %.4f @ %.2f", hedgeSide, hedgeQty, price),
			})
		}
	}
}

// ── Dynamic Spacing ──

// adjustSpacing dynamically adjusts grid spacing based on ATR volatility.
func (g *GridStrategy) adjustSpacing() {
	if g.currentATR == 0 || g.avgATR == 0 {
		return
	}

	// Volatility ratio: current ATR vs average ATR
	volRatio := g.currentATR / g.avgATR

	// Base spacing from config
	baseSpacing := g.spacingPct
	newSpacing := baseSpacing

	if volRatio > 1.5 {
		// High volatility: widen spacing (less levels, bigger profit per fill)
		newSpacing = baseSpacing * (1 + (volRatio-1.5)*0.3)
		if newSpacing > baseSpacing*2.0 {
			newSpacing = baseSpacing * 2.0
		}
	} else if volRatio < 0.6 {
		// Low volatility: tighten spacing (more levels, smaller profit but more fills)
		newSpacing = baseSpacing * (0.7 + volRatio*0.3)
	}

	newSpacing = math.Round(newSpacing*100) / 100
	if newSpacing != g.spacingPct && newSpacing >= 0.5 {
		log.Info().
			Float64("old_spacing", g.spacingPct).
			Float64("new_spacing", newSpacing).
			Float64("vol_ratio", volRatio).
			Msg("Dynamic spacing adjusted")

		g.spacingPct = newSpacing
		_ = g.rebuildGrid("dynamic_spacing")
	}
}

// ── State Persistence ──

// SaveState persists current grid state to a JSON file for crash recovery.
func (g *GridStrategy) SaveState() error {
	type GridState struct {
		Pair          string             `json:"pair"`
		HighPrice     float64            `json:"high_price"`
		LowPrice      float64            `json:"low_price"`
		SpacingPct    float64            `json:"spacing_pct"`
		EffectiveDir  string             `json:"effective_dir"`
		Orders        map[float64]string `json:"orders"`
		ActiveOrders  map[string]bool    `json:"active_orders"`
		Profit        float64            `json:"profit"`
		TotalTrades   int                `json:"total_trades"`
		StopLossPrice float64            `json:"stop_loss_price"`
		CurrentATR    float64            `json:"current_atr"`
		StartEquity   float64            `json:"start_equity"`
		StartTime     time.Time          `json:"start_time"`
		RebuildCount  int                `json:"rebuild_count"`
	}

	state := GridState{
		Pair:          g.pair,
		HighPrice:     g.highPrice,
		LowPrice:      g.lowPrice,
		SpacingPct:    g.spacingPct,
		EffectiveDir:  string(g.effectiveDir),
		Orders:        g.orders,
		ActiveOrders:  g.activeOrders,
		Profit:        g.profit,
		TotalTrades:   g.totalTrades,
		StopLossPrice: g.stopLossPrice,
		CurrentATR:    g.currentATR,
		StartEquity:   g.startEquity,
		StartTime:     g.startTime,
		RebuildCount:  g.rebuildCount,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("grid_state.json", data, 0644)
}

// LoadState restores grid state from saved JSON file.
func (g *GridStrategy) LoadState() error {
	data, err := os.ReadFile("grid_state.json")
	if err != nil {
		return err
	}

	var state struct {
		Pair          string             `json:"pair"`
		HighPrice     float64            `json:"high_price"`
		LowPrice      float64            `json:"low_price"`
		SpacingPct    float64            `json:"spacing_pct"`
		EffectiveDir  string             `json:"effective_dir"`
		Orders        map[float64]string `json:"orders"`
		ActiveOrders  map[string]bool    `json:"active_orders"`
		Profit        float64            `json:"profit"`
		TotalTrades   int                `json:"total_trades"`
		StopLossPrice float64            `json:"stop_loss_price"`
		CurrentATR    float64            `json:"current_atr"`
		StartEquity   float64            `json:"start_equity"`
		StartTime     time.Time          `json:"start_time"`
		RebuildCount  int                `json:"rebuild_count"`
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	g.pair = state.Pair
	g.highPrice = state.HighPrice
	g.lowPrice = state.LowPrice
	g.spacingPct = state.SpacingPct
	g.effectiveDir = Direction(state.EffectiveDir)
	g.orders = state.Orders
	g.activeOrders = state.ActiveOrders
	g.profit = state.Profit
	g.totalTrades = state.TotalTrades
	g.stopLossPrice = state.StopLossPrice
	g.currentATR = state.CurrentATR
	g.startEquity = state.StartEquity
	g.startTime = state.StartTime
	g.rebuildCount = state.RebuildCount

	log.Info().
		Str("pair", g.pair).
		Float64("profit", g.profit).
		Int("trades", g.totalTrades).
		Msg("Grid state restored from file")

	return nil
}

// ── Helpers ──

func calcATR(candles []exchange.Candle, period int) float64 {
	if len(candles) < period+1 {
		return 0
	}
	var sum float64
	for i := len(candles) - period; i < len(candles); i++ {
		prev := candles[i-1]
		curr := candles[i]
		tr := math.Max(curr.High-curr.Low, math.Max(math.Abs(curr.High-prev.Close), math.Abs(curr.Low-prev.Close)))
		sum += tr
	}
	return sum / float64(period)
}

func calcEMA(closes []float64, period int) float64 {
	if len(closes) < period {
		return closes[len(closes)-1]
	}
	k := 2.0 / float64(period+1)
	ema := 0.0
	for i := 0; i < period; i++ {
		ema += closes[i]
	}
	ema /= float64(period)
	for i := period; i < len(closes); i++ {
		ema = closes[i]*k + ema*(1-k)
	}
	return ema
}
