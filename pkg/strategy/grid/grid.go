package grid

import (
	"fmt"
	"math"

	"github.com/bengalm/mkk/pkg/eventbus"
	"github.com/bengalm/mkk/pkg/exchange"
	"github.com/bengalm/mkk/pkg/strategy"
	"github.com/rs/zerolog/log"
)

func init() {
	strategy.Register("grid", func() strategy.Strategy { return &GridStrategy{} })
}

// GridStrategy implements grid trading.
type GridStrategy struct {
	strategy.BaseStrategy
	pair             string
	highPrice        float64
	lowPrice         float64
	gridLevels       int
	quantityPerGrid  float64
	gridStep         float64
	orders           map[float64]string // price -> orderID
	activeOrders     map[string]bool    // orderID -> active
	profit           float64
	totalTrades      int
}

// Name returns strategy name.
func (g *GridStrategy) Name() string { return "grid" }

// Init initializes the grid strategy.
func (g *GridStrategy) Init(config map[string]interface{}, ex exchange.Exchange, bus *eventbus.EventBus) error {
	g.InitBase("grid", ex, bus)

	g.pair = strategy.GetStringParam(config, "pair", "BTC-USDT")
	g.highPrice = strategy.GetFloatParam(config, "high_price", 70000)
	g.lowPrice = strategy.GetFloatParam(config, "low_price", 60000)
	g.gridLevels = strategy.GetIntParam(config, "grid_levels", 20)
	g.quantityPerGrid = strategy.GetFloatParam(config, "quantity_per_grid", 0.001)
	g.gridStep = (g.highPrice - g.lowPrice) / float64(g.gridLevels)
	g.orders = make(map[float64]string)
	g.activeOrders = make(map[string]bool)

	log.Info().
		Str("pair", g.pair).
		Float64("high", g.highPrice).
		Float64("low", g.lowPrice).
		Int("levels", g.gridLevels).
		Float64("step", g.gridStep).
		Msg("Grid strategy initialized")

	// Place initial grid orders
	return g.placeGridOrders()
}

// placeGridOrders creates buy/sell orders at each grid level.
func (g *GridStrategy) placeGridOrders() error {
	ticker, err := g.Exchange().GetTicker(g.pair)
	if err != nil {
		return fmt.Errorf("get ticker for grid init: %w", err)
	}

	for i := 0; i < g.gridLevels; i++ {
		price := g.lowPrice + float64(i)*g.gridStep

		if price >= ticker.Last {
			// Above current price: place sell orders
			order, err := g.Exchange().PlaceOrder(exchange.OrderRequest{
				Pair:   g.pair,
				Side:   exchange.Sell,
				Type:   exchange.OrderLimit,
				Price:  price,
				Amount: g.quantityPerGrid,
			})
			if err != nil {
				log.Error().Float64("price", price).Err(err).Msg("grid sell order failed")
				continue
			}
			g.orders[price] = order.ID
			g.activeOrders[order.ID] = true
		} else {
			// Below current price: place buy orders
			order, err := g.Exchange().PlaceOrder(exchange.OrderRequest{
				Pair:   g.pair,
				Side:   exchange.Buy,
				Type:   exchange.OrderLimit,
				Price:  price,
				Amount: g.quantityPerGrid,
			})
			if err != nil {
				log.Error().Float64("price", price).Err(err).Msg("grid buy order failed")
				continue
			}
			g.orders[price] = order.ID
			g.activeOrders[order.ID] = true
		}
	}

	log.Info().Int("placed", len(g.orders)).Msg("Grid orders placed")
	return nil
}

// OnTick handles ticker updates.
func (g *GridStrategy) OnTick(ticker exchange.Ticker) {
	// Grid strategy is primarily order-driven, tick updates are informational
}

// OnCandle handles new candle data.
func (g *GridStrategy) OnCandle(candle exchange.Candle) {
	// Check if price moved outside grid range
	if candle.Close > g.highPrice*1.05 || candle.Close < g.lowPrice*0.95 {
		log.Warn().
			Float64("close", candle.Close).
			Float64("grid_high", g.highPrice).
			Float64("grid_low", g.lowPrice).
			Msg("Price outside grid range")
	}
}

// OnFill handles order fills.
func (g *GridStrategy) OnFill(trade exchange.Trade) {
	if !g.activeOrders[trade.OrderID] {
		return
	}
	delete(g.activeOrders, trade.OrderID)

	// Find the grid level
	var filledPrice float64
	for price, id := range g.orders {
		if id == trade.OrderID {
			filledPrice = price
			delete(g.orders, price)
			break
		}
	}

	// Place opposite order
	if trade.Side == exchange.Buy {
		// Buy filled, place sell at next grid level up
		sellPrice := filledPrice + g.gridStep
		if sellPrice <= g.highPrice {
			order, err := g.Exchange().PlaceOrder(exchange.OrderRequest{
				Pair:   g.pair,
				Side:   exchange.Sell,
				Type:   exchange.OrderLimit,
				Price:  sellPrice,
				Amount: g.quantityPerGrid,
			})
			if err != nil {
				log.Error().Float64("price", sellPrice).Err(err).Msg("grid sell replacement failed")
			} else {
				g.orders[sellPrice] = order.ID
				g.activeOrders[order.ID] = true
				profit := g.gridStep * g.quantityPerGrid
				g.profit += profit
				g.totalTrades++
				log.Info().
					Float64("buy_price", filledPrice).
					Float64("sell_price", sellPrice).
					Float64("profit", profit).
					Float64("total_profit", g.profit).
					Msg("Grid cycle completed")
			}
		}
	} else {
		// Sell filled, place buy at next grid level down
		buyPrice := filledPrice - g.gridStep
		if buyPrice >= g.lowPrice {
			order, err := g.Exchange().PlaceOrder(exchange.OrderRequest{
				Pair:   g.pair,
				Side:   exchange.Buy,
				Type:   exchange.OrderLimit,
				Price:  buyPrice,
				Amount: g.quantityPerGrid,
			})
			if err != nil {
				log.Error().Float64("price", buyPrice).Err(err).Msg("grid buy replacement failed")
			} else {
				g.orders[buyPrice] = order.ID
				g.activeOrders[order.ID] = true
				profit := g.gridStep * g.quantityPerGrid
				g.profit += profit
				g.totalTrades++
				log.Info().
					Float64("sell_price", filledPrice).
					Float64("buy_price", buyPrice).
					Float64("profit", profit).
					Float64("total_profit", g.profit).
					Msg("Grid cycle completed")
			}
		}
	}

	g.EmitSignal(strategy.TradeSignal{
		Action:   strategy.ActionHold,
		Pair:     g.pair,
		Price:    filledPrice,
		Strategy: "grid",
		Reason:   fmt.Sprintf("grid fill: %s @ %.2f, total_profit: %.4f", trade.Side, filledPrice, g.profit),
	})
}

// Stop cancels all grid orders.
func (g *GridStrategy) Stop() {
	for orderID := range g.activeOrders {
		if err := g.Exchange().CancelOrder(g.pair, orderID); err != nil {
			log.Error().Str("orderID", orderID).Err(err).Msg("cancel grid order failed")
		}
	}
	g.BaseStrategy.Stop()
	log.Info().
		Float64("total_profit", g.profit).
		Int("total_trades", g.totalTrades).
		Msg("Grid strategy stopped")
}

// Stats returns current grid statistics.
func (g *GridStrategy) Stats() map[string]interface{} {
	return map[string]interface{}{
		"pair":          g.pair,
		"high_price":    g.highPrice,
		"low_price":     g.lowPrice,
		"grid_levels":   g.gridLevels,
		"grid_step":     g.gridStep,
		"active_orders": len(g.activeOrders),
		"total_profit":  math.Round(g.profit*100) / 100,
		"total_trades":  g.totalTrades,
	}
}
