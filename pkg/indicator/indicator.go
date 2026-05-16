package indicator

import "math"

// SMA computes Simple Moving Average.
func SMA(data []float64, period int) []float64 {
	if len(data) < period {
		return nil
	}
	result := make([]float64, len(data)-period+1)
	for i := period - 1; i < len(data); i++ {
		sum := 0.0
		for j := i - period + 1; j <= i; j++ {
			sum += data[j]
		}
		result[i-period+1] = sum / float64(period)
	}
	return result
}

// EMA computes Exponential Moving Average.
func EMA(data []float64, period int) []float64 {
	if len(data) < period {
		return nil
	}
	result := make([]float64, len(data))
	multiplier := 2.0 / float64(period+1)

	// First EMA value is SMA
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += data[i]
	}
	result[period-1] = sum / float64(period)

	for i := period; i < len(data); i++ {
		result[i] = (data[i]-result[i-1])*multiplier + result[i-1]
	}
	return result[period-1:]
}

// RSI computes Relative Strength Index.
func RSI(data []float64, period int) []float64 {
	if len(data) < period+1 {
		return nil
	}
	result := make([]float64, len(data)-period)
	gains := make([]float64, len(data)-1)
	losses := make([]float64, len(data)-1)

	for i := 1; i < len(data); i++ {
		change := data[i] - data[i-1]
		if change > 0 {
			gains[i-1] = change
		} else {
			losses[i-1] = -change
		}
	}

	avgGain := 0.0
	avgLoss := 0.0
	for i := 0; i < period; i++ {
		avgGain += gains[i]
		avgLoss += losses[i]
	}
	avgGain /= float64(period)
	avgLoss /= float64(period)

	if avgLoss == 0 {
		result[0] = 100
	} else {
		rs := avgGain / avgLoss
		result[0] = 100 - 100/(1+rs)
	}

	for i := 1; i < len(result); i++ {
		idx := period + i - 1
		avgGain = (avgGain*float64(period-1) + gains[idx]) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + losses[idx]) / float64(period)
		if avgLoss == 0 {
			result[i] = 100
		} else {
			rs := avgGain / avgLoss
			result[i] = 100 - 100/(1+rs)
		}
	}
	return result
}

// MACD computes Moving Average Convergence Divergence.
// Returns (macd, signal, histogram).
func MACD(data []float64, fast, slow, signalPeriod int) ([]float64, []float64, []float64) {
	emaFast := EMA(data, fast)
	emaSlow := EMA(data, slow)
	if emaFast == nil || emaSlow == nil {
		return nil, nil, nil
	}

	// Align lengths
	minLen := min(len(emaFast), len(emaSlow))
	emaFast = emaFast[len(emaFast)-minLen:]
	emaSlow = emaSlow[len(emaSlow)-minLen:]

	macdLine := make([]float64, minLen)
	for i := 0; i < minLen; i++ {
		macdLine[i] = emaFast[i] - emaSlow[i]
	}

	signalLine := EMA(macdLine, signalPeriod)
	if signalLine == nil {
		return macdLine, nil, nil
	}

	macdLine = macdLine[len(macdLine)-len(signalLine):]
	histogram := make([]float64, len(signalLine))
	for i := 0; i < len(signalLine); i++ {
		histogram[i] = macdLine[i] - signalLine[i]
	}
	return macdLine, signalLine, histogram
}

// BollingerBands computes Bollinger Bands.
// Returns (upper, middle, lower).
func BollingerBands(data []float64, period int, stdDev float64) ([]float64, []float64, []float64) {
	middle := SMA(data, period)
	if middle == nil {
		return nil, nil, nil
	}
	upper := make([]float64, len(middle))
	lower := make([]float64, len(middle))

	for i := 0; i < len(middle); i++ {
		idx := i + period - 1
		variance := 0.0
		for j := idx - period + 1; j <= idx; j++ {
			diff := data[j] - middle[i]
			variance += diff * diff
		}
		std := math.Sqrt(variance / float64(period))
		upper[i] = middle[i] + stdDev*std
		lower[i] = middle[i] - stdDev*std
	}
	return upper, middle, lower
}

// ATR computes Average True Range.
func ATR(highs, lows, closes []float64, period int) []float64 {
	n := len(closes)
	if n < period+1 {
		return nil
	}
	tr := make([]float64, n-1)
	for i := 1; i < n; i++ {
		tr[i-1] = math.Max(
			highs[i]-lows[i],
			math.Max(
				math.Abs(highs[i]-closes[i-1]),
				math.Abs(lows[i]-closes[i-1]),
			),
		)
	}

	result := make([]float64, len(tr)-period+1)
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += tr[i]
	}
	result[0] = sum / float64(period)

	for i := 1; i < len(result); i++ {
		result[i] = (result[i-1]*float64(period-1) + tr[period+i-1]) / float64(period)
	}
	return result
}

// StochRSI computes Stochastic RSI.
// Returns (k, d).
func StochRSI(data []float64, rsiPeriod, stochPeriod, kSmooth, dSmooth int) ([]float64, []float64) {
	rsi := RSI(data, rsiPeriod)
	if rsi == nil || len(rsi) < stochPeriod {
		return nil, nil
	}

	rawK := make([]float64, len(rsi)-stochPeriod+1)
	for i := stochPeriod - 1; i < len(rsi); i++ {
		minRSI := rsi[i]
		maxRSI := rsi[i]
		for j := i - stochPeriod + 1; j <= i; j++ {
			if rsi[j] < minRSI {
				minRSI = rsi[j]
			}
			if rsi[j] > maxRSI {
				maxRSI = rsi[j]
			}
		}
		if maxRSI == minRSI {
			rawK[i-stochPeriod+1] = 0
		} else {
			rawK[i-stochPeriod+1] = (rsi[i] - minRSI) / (maxRSI - minRSI) * 100
		}
	}

	k := SMA(rawK, kSmooth)
	d := SMA(k, dSmooth)
	return k, d
}

// VWAP computes Volume Weighted Average Price.
func VWAP(highs, lows, closes, volumes []float64) []float64 {
	n := len(closes)
	if n == 0 {
		return nil
	}
	result := make([]float64, n)
	cumVP := 0.0
	cumVol := 0.0
	for i := 0; i < n; i++ {
		tp := (highs[i] + lows[i] + closes[i]) / 3
		cumVP += tp * volumes[i]
		cumVol += volumes[i]
		if cumVol > 0 {
			result[i] = cumVP / cumVol
		}
	}
	return result
}

// SuperTrend computes SuperTrend indicator.
// Returns (superTrend, direction) where direction: 1=uptrend, -1=downtrend.
func SuperTrend(highs, lows, closes []float64, period int, multiplier float64) ([]float64, []int) {
	atr := ATR(highs, lows, closes, period)
	if atr == nil {
		return nil, nil
	}

	n := len(closes)
	st := make([]float64, n)
	dir := make([]int, n)

	offset := n - len(atr)
	for i := 0; i < len(atr); i++ {
		idx := i + offset
		hl2 := (highs[idx] + lows[idx]) / 2
		up := hl2 - multiplier*atr[i]
		dn := hl2 + multiplier*atr[i]

		if i == 0 {
			if closes[idx] > hl2 {
				st[idx] = up
				dir[idx] = 1
			} else {
				st[idx] = dn
				dir[idx] = -1
			}
			continue
		}

		prevST := st[idx-1]
		// Adjust bands
		if up < prevST || closes[idx-1] < prevST {
			// keep upper band
		} else {
			up = prevST
		}
		if dn > prevST || closes[idx-1] > prevST {
			// keep lower band
		} else {
			dn = prevST
		}

		if dir[idx-1] == 1 {
			if closes[idx] < up {
				st[idx] = dn
				dir[idx] = -1
			} else {
				st[idx] = up
				dir[idx] = 1
			}
		} else {
			if closes[idx] > dn {
				st[idx] = up
				dir[idx] = 1
			} else {
				st[idx] = dn
				dir[idx] = -1
			}
		}
	}

	// Fill leading zeros
	for i := 0; i < offset; i++ {
		st[i] = 0
		dir[i] = 0
	}
	return st, dir
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
