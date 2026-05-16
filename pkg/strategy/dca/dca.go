package dca

import (
	"fmt"
	"time"

	"github.com/bengalm/mkk/pkg/eventbus"
	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/rs/zerolog/log"
)

func init() {
	strategy.Register("dca", func() strategy.Strategy { return &DCAStrategy{} })
}

// DCAStrategy implements Dollar Cost Averaging.
type DCAStrategy struct {
	strategy.BaseStrategy
	pair           string
	totalInvest    float64 // total investment amount in quote currency
	orderCount     int     // number of DCA orders
	interval       string  // time between orders (e.g., "1h", "1d")
	priceDeviation float64 // extra order threshold (%)
	takeProfit     float64 // take profit %
	stopLoss       float64 // stop loss %
	ordersPlaced   int
	totalSpent     float64
	totalAmount    float64 // base currency
	avgPrice       float64
	entryPrices    []float64
	lastOrderTime  time.Time
}

// Name returns strategy name.
func (d *DCAStrategy) Name() string { return "dca" }

// Init initializes the DCA strategy.
func (d *DCAStrategy) Init(config map[string]interface{}, ex exchange.Exchange, bus *eventbus.EventBus) error {
	d.InitBase("dca", ex, bus)

	d.pair = strategy.GetStringParam(config, "pair", "BTC-USDT")
	d.totalInvest = strategy.GetFloatParam(config, "total_investment", 1000)
	d.orderCount = strategy.GetIntParam(config, "order_count", 10)
	d.interval = strategy.GetStringParam(config, "interval", "1h")
	d.priceDeviation = strategy.GetFloatParam(config, "price_deviation", 0.03) // 3%
	d.takeProfit = strategy.GetFloatParam(config, "take_profit", 0.10)         // 10%
	d.stopLoss = strategy.GetFloatParam(config, "stop_loss", 0.15)             // 15%
	d.entryPrices = make([]float64, 0)

	log.Info().
		Str("pair", d.pair).
		Float64("total_invest", d.totalInvest).
		Int("orders", d.orderCount).
		Str("interval", d.interval).
		Msg("DCA strategy initialized")

	// Place first order immediately
	return d.placeDCAOrder()
}

// placeDCAOrder places a DCA buy order.
func (d *DCAStrategy) placeDCAOrder() error {
	if d.ordersPlaced >= d.orderCount {
		log.Info().Msg("DCA: all orders placed")
		return nil
	}

	orderAmount := d.totalInvest / float64(d.orderCount)

	ticker, err := d.Exchange().GetTicker(d.pair)
	if err != nil {
		return fmt.Errorf("get ticker: %w", err)
	}

	// Calculate base amount
	amount := orderAmount / ticker.Last

	_, err = d.Exchange().PlaceOrder(exchange.OrderRequest{
		Pair:   d.pair,
		Side:   exchange.Buy,
		Type:   exchange.OrderMarket,
		Amount: amount,
	})
	if err != nil {
		return fmt.Errorf("place DCA order: %w", err)
	}

	d.ordersPlaced++
	d.totalSpent += orderAmount
	d.totalAmount += amount
	d.avgPrice = d.totalSpent / d.totalAmount
	d.entryPrices = append(d.entryPrices, ticker.Last)
	d.lastOrderTime = time.Now()

	log.Info().
		Int("order_num", d.ordersPlaced).
		Int("total_orders", d.orderCount).
		Float64("price", ticker.Last).
		Float64("amount", amount).
		Float64("avg_price", d.avgPrice).
		Float64("total_spent", d.totalSpent).
		Msg("DCA order placed")

	d.EmitSignal(strategy.TradeSignal{
		Action:   strategy.ActionBuy,
		Pair:     d.pair,
		Price:    ticker.Last,
		Amount:   amount,
		Strategy: "dca",
		Reason:   fmt.Sprintf("DCA order #%d at %.2f, avg: %.2f", d.ordersPlaced, ticker.Last, d.avgPrice),
	})

	return nil
}

// OnTick checks for take-profit and stop-loss.
func (d *DCAStrategy) OnTick(ticker exchange.Ticker) {
	if d.totalAmount == 0 {
		return
	}

	pnl := (ticker.Last - d.avgPrice) / d.avgPrice

	// Take profit
	if d.takeProfit > 0 && pnl >= d.takeProfit {
		log.Info().
			Float64("pnl_pct", pnl*100).
			Float64("profit", d.totalAmount*ticker.Last-d.totalSpent).
			Msg("DCA take profit triggered")

		d.EmitSignal(strategy.TradeSignal{
			Action:     strategy.ActionSell,
			Pair:       d.pair,
			Price:      ticker.Last,
			Amount:     d.totalAmount,
			Type:       exchange.OrderMarket,
			TakeProfit: d.avgPrice * (1 + d.takeProfit),
			Strategy:   "dca",
			Reason:     fmt.Sprintf("TP: pnl=%.2f%%", pnl*100),
		})
		return
	}

	// Stop loss
	if d.stopLoss > 0 && pnl <= -d.stopLoss {
		log.Warn().
			Float64("pnl_pct", pnl*100).
			Float64("loss", d.totalAmount*ticker.Last-d.totalSpent).
			Msg("DCA stop loss triggered")

		d.EmitSignal(strategy.TradeSignal{
			Action:   strategy.ActionSell,
			Pair:     d.pair,
			Price:    ticker.Last,
			Amount:   d.totalAmount,
			Type:     exchange.OrderMarket,
			StopLoss: d.avgPrice * (1 - d.stopLoss),
			Strategy: "dca",
			Reason:   fmt.Sprintf("SL: pnl=%.2f%%", pnl*100),
		})
		return
	}

	// Price deviation check — buy more if price drops
	if d.priceDeviation > 0 && len(d.entryPrices) > 0 {
		lastEntry := d.entryPrices[len(d.entryPrices)-1]
		if ticker.Last < lastEntry*(1-d.priceDeviation) && d.ordersPlaced < d.orderCount {
			log.Info().
				Float64("drop", (1-ticker.Last/lastEntry)*100).
				Msg("Price deviation triggered extra DCA")
			d.placeDCAOrder()
		}
	}
}

// OnCandle checks if it's time for the next DCA order.
func (d *DCAStrategy) OnCandle(candle exchange.Candle) {
	if d.ordersPlaced >= d.orderCount {
		return
	}

	intervalDur, err := time.ParseDuration(d.interval)
	if err != nil {
		// Try common formats
		switch d.interval {
		case "1d", "daily":
			intervalDur = 24 * time.Hour
		default:
			intervalDur = time.Hour
		}
	}

	if time.Since(d.lastOrderTime) >= intervalDur {
		d.placeDCAOrder()
	}
}

// OnFill handles order fills.
func (d *DCAStrategy) OnFill(trade exchange.Trade) {
	// Handled in placeDCAOrder
}

// Stats returns DCA statistics.
func (d *DCAStrategy) Stats() map[string]interface{} {
	return map[string]interface{}{
		"pair":          d.pair,
		"orders_placed": d.ordersPlaced,
		"total_orders":  d.orderCount,
		"total_spent":   d.totalSpent,
		"total_amount":  d.totalAmount,
		"avg_price":     d.avgPrice,
	}
}
