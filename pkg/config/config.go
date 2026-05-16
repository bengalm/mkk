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
