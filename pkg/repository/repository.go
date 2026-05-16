package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bengalm/mkk/pkg/exchange"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// Repository provides data persistence.
type Repository struct {
	db *sql.DB
}

// New creates a new repository and runs migrations.
func New(dbPath string) (*Repository, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	r := &Repository{db: db}
	if err := r.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	log.Info().Str("path", dbPath).Msg("Repository initialized")
	return r, nil
}

// Close closes the database.
func (r *Repository) Close() error {
	return r.db.Close()
}

func (r *Repository) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS trades (
			id TEXT PRIMARY KEY,
			pair TEXT NOT NULL,
			side TEXT NOT NULL,
			price REAL NOT NULL,
			amount REAL NOT NULL,
			pnl REAL DEFAULT 0,
			fee REAL DEFAULT 0,
			reason TEXT DEFAULT '',
			strategy TEXT DEFAULT '',
			timestamp DATETIME NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS positions (
			pair TEXT PRIMARY KEY,
			side TEXT NOT NULL,
			entry_price REAL NOT NULL,
			amount REAL NOT NULL,
			stop_loss REAL DEFAULT 0,
			take_profit REAL DEFAULT 0,
			strategy TEXT DEFAULT '',
			order_id TEXT DEFAULT '',
			opened_at DATETIME NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS candles (
			pair TEXT NOT NULL,
			timeframe TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			open REAL NOT NULL,
			high REAL NOT NULL,
			low REAL NOT NULL,
			close REAL NOT NULL,
			volume REAL NOT NULL,
			PRIMARY KEY (pair, timeframe, timestamp)
		)`,
		`CREATE TABLE IF NOT EXISTS signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			strategy TEXT NOT NULL,
			action TEXT NOT NULL,
			pair TEXT NOT NULL,
			price REAL NOT NULL,
			amount REAL NOT NULL,
			reason TEXT DEFAULT '',
			executed INTEGER DEFAULT 0,
			timestamp DATETIME NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS backtest_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			strategy TEXT NOT NULL,
			pair TEXT NOT NULL,
			timeframe TEXT NOT NULL,
			start_time DATETIME NOT NULL,
			end_time DATETIME NOT NULL,
			initial_balance REAL NOT NULL,
			final_balance REAL NOT NULL,
			total_trades INTEGER DEFAULT 0,
			win_rate REAL DEFAULT 0,
			max_drawdown REAL DEFAULT 0,
			profit_factor REAL DEFAULT 0,
			total_pnl REAL DEFAULT 0,
			result_json TEXT DEFAULT '',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_candles_pair_tf ON candles(pair, timeframe)`,
		`CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy)`,
	}

	for _, m := range migrations {
		if _, err := r.db.Exec(m); err != nil {
			return fmt.Errorf("exec migration: %w", err)
		}
	}
	return nil
}

// SaveTrade saves a trade record.
func (r *Repository) SaveTrade(trade *exchange.Trade, pnl, fee float64, reason, strategy string) error {
	_, err := r.db.Exec(`
		INSERT OR REPLACE INTO trades (id, pair, side, price, amount, pnl, fee, reason, strategy, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		trade.ID, trade.Pair, string(trade.Side), trade.Price, trade.Amount,
		pnl, fee, reason, strategy, trade.Timestamp,
	)
	return err
}

// SavePosition saves or updates a position.
func (r *Repository) SavePosition(pos *exchange.Position, strategy, orderID string, stopLoss, takeProfit float64) error {
	_, err := r.db.Exec(`
		INSERT OR REPLACE INTO positions (pair, side, entry_price, amount, stop_loss, take_profit, strategy, order_id, opened_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pos.Pair, string(pos.Side), pos.EntryPrice, pos.Size,
		stopLoss, takeProfit, strategy, orderID,
		pos.Timestamp, time.Now(),
	)
	return err
}

// DeletePosition removes a closed position.
func (r *Repository) DeletePosition(pair string) error {
	_, err := r.db.Exec(`DELETE FROM positions WHERE pair = ?`, pair)
	return err
}

// GetOpenPositions returns all tracked open positions.
func (r *Repository) GetOpenPositions() ([]map[string]interface{}, error) {
	rows, err := r.db.Query(`SELECT pair, side, entry_price, amount, stop_loss, take_profit, strategy, order_id, opened_at FROM positions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var pair, side, strategy, orderID, openedAt string
		var entryPrice, amount, stopLoss, takeProfit float64
		if err := rows.Scan(&pair, &side, &entryPrice, &amount, &stopLoss, &takeProfit, &strategy, &orderID, &openedAt); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"pair":        pair,
			"side":        side,
			"entry_price": entryPrice,
			"amount":      amount,
			"stop_loss":   stopLoss,
			"take_profit": takeProfit,
			"strategy":    strategy,
			"order_id":    orderID,
			"opened_at":   openedAt,
		})
	}
	return results, nil
}

// SaveCandles saves candle data (batch upsert).
func (r *Repository) SaveCandles(candles []exchange.Candle, pair, timeframe string) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO candles (pair, timeframe, timestamp, open, high, low, close, volume)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, c := range candles {
		_, err := stmt.Exec(pair, timeframe, c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume)
		if err != nil {
			log.Warn().Err(err).Time("ts", c.Timestamp).Msg("Failed to save candle")
		}
	}
	return tx.Commit()
}

// GetCandles retrieves stored candles.
func (r *Repository) GetCandles(pair, timeframe string, start, end time.Time, limit int) ([]exchange.Candle, error) {
	query := `SELECT timestamp, open, high, low, close, volume FROM candles
		WHERE pair = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ?
		ORDER BY timestamp ASC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := r.db.Query(query, pair, timeframe, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candles []exchange.Candle
	for rows.Next() {
		var c exchange.Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume); err != nil {
			continue
		}
		c.Pair = pair
		c.Timeframe = timeframe
		candles = append(candles, c)
	}
	return candles, nil
}

// SaveSignal saves a strategy signal.
func (r *Repository) SaveSignal(strategy, action, pair string, price, amount float64, reason string, executed bool) error {
	_, err := r.db.Exec(`
		INSERT INTO signals (strategy, action, pair, price, amount, reason, executed, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		strategy, action, pair, price, amount, reason, executed, time.Now(),
	)
	return err
}

// SaveBacktestResult saves a backtest result.
func (r *Repository) SaveBacktestResult(result map[string]interface{}) error {
	jsonData, _ := json.Marshal(result)
	_, err := r.db.Exec(`
		INSERT INTO backtest_results (strategy, pair, timeframe, start_time, end_time,
			initial_balance, final_balance, total_trades, win_rate, max_drawdown, profit_factor, total_pnl, result_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		result["strategy"], result["pair"], result["timeframe"],
		result["start_time"], result["end_time"],
		result["initial_balance"], result["final_balance"],
		result["total_trades"], result["win_rate"],
		result["max_drawdown"], result["profit_factor"], result["total_pnl"],
		string(jsonData),
	)
	return err
}

// GetTradeHistory returns recent trades.
func (r *Repository) GetTradeHistory(limit int) ([]map[string]interface{}, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.Query(`
		SELECT id, pair, side, price, amount, pnl, fee, reason, strategy, timestamp
		FROM trades ORDER BY timestamp DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, pair, side, reason, strategy string
		var price, amount, pnl, fee float64
		var ts time.Time
		if err := rows.Scan(&id, &pair, &side, &price, &amount, &pnl, &fee, &reason, &strategy, &ts); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"id": id, "pair": pair, "side": side, "price": price,
			"amount": amount, "pnl": pnl, "fee": fee,
			"reason": reason, "strategy": strategy, "timestamp": ts,
		})
	}
	return results, nil
}

// GetPnLSummary returns daily P&L summary.
func (r *Repository) GetPnLSummary(days int) ([]map[string]interface{}, error) {
	since := time.Now().AddDate(0, 0, -days)
	rows, err := r.db.Query(`
		SELECT DATE(timestamp) as day, COUNT(*) as trades,
			SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
			SUM(pnl) as total_pnl,
			AVG(pnl) as avg_pnl
		FROM trades WHERE timestamp >= ? AND reason != 'open'
		GROUP BY DATE(timestamp) ORDER BY day DESC`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var day string
		var trades, wins int
		var totalPnL, avgPnL float64
		if err := rows.Scan(&day, &trades, &wins, &totalPnL, &avgPnL); err != nil {
			continue
		}
		winRate := 0.0
		if trades > 0 {
			winRate = float64(wins) / float64(trades) * 100
		}
		results = append(results, map[string]interface{}{
			"date": day, "trades": trades, "wins": wins,
			"win_rate": winRate, "total_pnl": totalPnL, "avg_pnl": avgPnL,
		})
	}
	return results, nil
}
