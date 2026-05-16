package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all application configuration.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Exchange ExchangeConfig `yaml:"exchange"`
	Strategy StrategyConfig `yaml:"strategy"`
	Logging  LoggingConfig  `yaml:"logging"`
	Database DatabaseConfig `yaml:"database"`
	Paper    bool           `yaml:"paper"`
	Risk     RiskConfig     `yaml:"risk"`
	Notifier NotifierConfig `yaml:"notifier"`
}

// RiskConfig holds risk management parameters for live trading.
type RiskConfig struct {
	MaxPositionUSDT  float64 `yaml:"max_position_usdt"` // max USDT per position
	MaxDailyLoss     float64 `yaml:"max_daily_loss"`    // max daily loss USDT
	MaxDrawdownPct   float64 `yaml:"max_drawdown_pct"`  // max drawdown %
	MaxOpenPositions int     `yaml:"max_open_positions"`
	DefaultLeverage  int     `yaml:"default_leverage"`
	MinRiskReward    float64 `yaml:"min_risk_reward"`
	MaxLossPerTrade  float64 `yaml:"max_loss_per_trade"` // hard cap per trade (e.g. 15)
	DailyMaxTrades   int     `yaml:"daily_max_trades"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

// ExchangeConfig holds exchange API credentials.
type ExchangeConfig struct {
	Name       string `yaml:"name"`       // "okx"
	APIKey     string `yaml:"api_key"`
	SecretKey  string `yaml:"secret_key"`
	Passphrase string `yaml:"passphrase"`
	Testnet    bool   `yaml:"testnet"`
}

// StrategyConfig holds strategy selection and parameters.
type StrategyConfig struct {
	Active  string                 `yaml:"active"`  // "grid", "dca", "rsi"
	Name    string                 `yaml:"name"`    // alias for Active
	Pair    string                 `yaml:"pair"`    // "BTC-USDT"`
	Params  map[string]interface{} `yaml:"params"`
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	Level  string `yaml:"level"`  // "debug", "info", "warn", "error"
	Format string `yaml:"format"` // "console", "json"
}

// DatabaseConfig holds database settings.
type DatabaseConfig struct {
	Path string `yaml:"path"`
}

// NotifierConfig holds notification settings.
type NotifierConfig struct {
	Type     string `yaml:"type"`      // "telegram"
	BotToken string `yaml:"bot_token"` // Telegram bot token
	ChatID   string `yaml:"chat_id"`   // Telegram chat ID
}

// Enabled returns true if notifier is configured.
func (n NotifierConfig) Enabled() bool {
	return n.Type != "" && n.BotToken != "" && n.ChatID != ""
}

// DefaultConfig returns a config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port: 8080,
			Host: "0.0.0.0",
		},
		Exchange: ExchangeConfig{
			Name: "okx",
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "console",
		},
		Database: DatabaseConfig{
			Path: "mkk.db",
		},
		Paper: false,
		Risk: RiskConfig{
			MaxPositionUSDT:  100,
			MaxDailyLoss:     30,
			MaxDrawdownPct:   15,
			MaxOpenPositions: 3,
			DefaultLeverage:  3,
			MinRiskReward:    2.0,
			MaxLossPerTrade:  15,
			DailyMaxTrades:   2,
		},
	}
}

// Load reads config from a YAML file.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// Validate checks required config fields.
func (c *Config) Validate() error {
	if c.Exchange.Name == "" {
		return fmt.Errorf("exchange name is required")
	}
	// Accept either Active or Name as the strategy identifier
	if c.Strategy.Active == "" && c.Strategy.Name == "" {
		return fmt.Errorf("strategy name is required")
	}
	if c.Strategy.Active == "" {
		c.Strategy.Active = c.Strategy.Name
	}
	if !c.Paper {
		if c.Exchange.APIKey == "" {
			return fmt.Errorf("exchange api_key is required for live trading")
		}
		if c.Exchange.SecretKey == "" {
			return fmt.Errorf("exchange secret_key is required for live trading")
		}
		if c.Exchange.Passphrase == "" {
			return fmt.Errorf("exchange passphrase is required for live trading")
		}
	}
	return nil
}
