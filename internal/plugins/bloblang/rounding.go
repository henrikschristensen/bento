package bloblang

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/warpstreamlabs/bento/public/bloblang"
)

// roundFloat rounds v to the given number of decimal places using the specified style.
//
// Supported styles:
//   - half_up    – ties round towards +∞  (1.5→2, −1.5→−1)
//   - half_down  – ties round towards −∞  (1.5→1, −1.5→−2)
//   - half_even  – ties round to nearest even digit (banker's rounding)
//   - ceil       – always towards +∞
//   - floor      – always towards −∞
//   - truncate   – always towards zero
func roundFloat(v float64, precision int, style string) (float64, error) {
	factor := math.Pow(10, float64(precision))
	shifted := v * factor

	var result float64
	switch style {
	case "half_up":
		result = math.Floor(shifted+0.5) / factor
	case "half_down":
		result = math.Ceil(shifted-0.5) / factor
	case "half_even":
		floor := math.Floor(shifted)
		frac := shifted - floor
		switch {
		case frac < 0.5:
			result = floor / factor
		case frac > 0.5:
			result = math.Ceil(shifted) / factor
		default:
			// Exactly halfway – round to nearest even
			if math.Mod(floor, 2) == 0 {
				result = floor / factor
			} else {
				result = math.Ceil(shifted) / factor
			}
		}
	case "ceil":
		result = math.Ceil(shifted) / factor
	case "floor":
		result = math.Floor(shifted) / factor
	case "truncate":
		result = math.Trunc(shifted) / factor
	default:
		return 0, fmt.Errorf("unknown rounding style %q: must be one of half_up, half_down, half_even, ceil, floor, truncate", style)
	}

	return result, nil
}

// toFloat64 converts any numeric type to float64.
func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int:
		return float64(n), nil
	case int32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case uint:
		return float64(n), nil
	case uint32:
		return float64(n), nil
	case uint64:
		return float64(n), nil
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, fmt.Errorf("could not parse json.Number %q as float64: %w", n, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("expected a numeric value, got %T", v)
	}
}

// RegisterRounding registers the "round_precise" bloblang method on the global environment.
//
// Usage in a mapping:
//
//	root.value = this.value.round_precise(precision: 2, rounding_style: "half_even")
func RegisterRounding() {
	RegisterRoundingInEnv(nil)
}

// RegisterRoundingInEnv registers the "round_precise" bloblang method on the provided
// environment. Pass nil to register on the global environment.
func RegisterRoundingInEnv(env *bloblang.Environment) {
	spec := bloblang.NewPluginSpec().
		Description("Round a decimal number to the given precision using the specified rounding style.").
		Param(bloblang.NewInt64Param("precision").
			Description("Number of decimal places to round to. Use 0 for whole numbers, negative values to round to tens/hundreds/etc.").
			Default(int64(0))).
		Param(bloblang.NewStringParam("rounding_style").
			Description("Rounding style: half_up (default), half_down, half_even, ceil, floor, or truncate.").
			Default("half_up"))

	if env != nil {
		env.RegisterMethodV2("round_precise", spec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return makeRoundMethod(args)
		})
	} else {
		bloblang.RegisterMethodV2("round_precise", spec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return makeRoundMethod(args)
		})
	}
}

func makeRoundMethod(args *bloblang.ParsedParams) (bloblang.Method, error) {
	precision, err := args.GetInt64("precision")
	if err != nil {
		return nil, err
	}
	style, err := args.GetString("rounding_style")
	if err != nil {
		return nil, err
	}
	return func(v any) (any, error) {
		f, err := toFloat64(v)
		if err != nil {
			return nil, err
		}
		return roundFloat(f, int(precision), style)
	}, nil
}
