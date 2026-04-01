package bloblang

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

func TestRoundFloat(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		precision int
		style     string
		expected  float64
		wantErr   bool
	}{
		// half_up: ties go towards +∞
		{name: "half_up positive tie", value: 1.5, precision: 0, style: "half_up", expected: 2},
		{name: "half_up negative tie", value: -1.5, precision: 0, style: "half_up", expected: -1},
		{name: "half_up rounds down below tie", value: 1.4, precision: 0, style: "half_up", expected: 1},
		{name: "half_up rounds up above tie", value: 1.6, precision: 0, style: "half_up", expected: 2},
		{name: "half_up with precision", value: 1.125, precision: 2, style: "half_up", expected: 1.13},
		{name: "half_up zero", value: 0.0, precision: 2, style: "half_up", expected: 0},

		// half_down: ties go towards -∞
		{name: "half_down positive tie", value: 1.5, precision: 0, style: "half_down", expected: 1},
		{name: "half_down negative tie", value: -1.5, precision: 0, style: "half_down", expected: -2},
		{name: "half_down rounds down below tie", value: 1.4, precision: 0, style: "half_down", expected: 1},
		{name: "half_down rounds up above tie", value: 1.6, precision: 0, style: "half_down", expected: 2},

		// half_even: ties round to nearest even digit (banker's rounding)
		{name: "half_even tie to even (floor)", value: 0.5, precision: 0, style: "half_even", expected: 0},
		{name: "half_even tie to even (ceil)", value: 1.5, precision: 0, style: "half_even", expected: 2},
		{name: "half_even tie to even 2.5", value: 2.5, precision: 0, style: "half_even", expected: 2},
		{name: "half_even tie to even 3.5", value: 3.5, precision: 0, style: "half_even", expected: 4},
		{name: "half_even no tie rounds normally", value: 1.4, precision: 0, style: "half_even", expected: 1},
		{name: "half_even no tie rounds up", value: 1.6, precision: 0, style: "half_even", expected: 2},
		{name: "half_even with precision", value: 2.45, precision: 1, style: "half_even", expected: 2.4},

		// ceil: always towards +∞
		{name: "ceil positive", value: 1.1, precision: 0, style: "ceil", expected: 2},
		{name: "ceil negative", value: -1.9, precision: 0, style: "ceil", expected: -1},
		{name: "ceil exact", value: 2.0, precision: 0, style: "ceil", expected: 2},
		{name: "ceil with precision", value: 1.231, precision: 2, style: "ceil", expected: 1.24},

		// floor: always towards -∞
		{name: "floor positive", value: 1.9, precision: 0, style: "floor", expected: 1},
		{name: "floor negative", value: -1.1, precision: 0, style: "floor", expected: -2},
		{name: "floor exact", value: 2.0, precision: 0, style: "floor", expected: 2},
		{name: "floor with precision", value: 1.239, precision: 2, style: "floor", expected: 1.23},

		// truncate: always towards zero
		{name: "truncate positive", value: 1.9, precision: 0, style: "truncate", expected: 1},
		{name: "truncate negative", value: -1.9, precision: 0, style: "truncate", expected: -1},
		{name: "truncate with precision", value: -1.239, precision: 2, style: "truncate", expected: -1.23},

		// negative precision (round to tens, hundreds, etc.)
		{name: "negative precision tens", value: 1234.5, precision: -1, style: "half_up", expected: 1230},
		{name: "negative precision hundreds", value: 1234.5, precision: -2, style: "half_up", expected: 1200},

		// unknown style
		{name: "unknown style", value: 1.5, precision: 0, style: "banana", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := roundFloat(tc.value, tc.precision, tc.style)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected float64
		wantErr  bool
	}{
		{name: "float64", input: float64(3.14), expected: 3.14},
		{name: "float32", input: float32(3.14), expected: float64(float32(3.14))},
		{name: "int", input: int(42), expected: 42},
		{name: "int32", input: int32(42), expected: 42},
		{name: "int64", input: int64(42), expected: 42},
		{name: "uint", input: uint(42), expected: 42},
		{name: "uint32", input: uint32(42), expected: 42},
		{name: "uint64", input: uint64(42), expected: 42},
		{name: "json.Number integer", input: json.Number("42"), expected: 42},
		{name: "json.Number float", input: json.Number("3.14"), expected: 3.14},
		{name: "json.Number invalid errors", input: json.Number("not-a-number"), wantErr: true},
		
		{name: "nil errors", input: nil, wantErr: true},
		{name: "bool errors", input: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := toFloat64(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestRoundPreciseBloblangMethod(t *testing.T) {
	env := bloblang.NewEnvironment()
	RegisterRoundingInEnv(env)

	tests := []struct {
		name     string
		mapping  string
		input    any
		expected any
		wantErr  bool
	}{
		{
			name:     "default params",
			mapping:  `root = this.round_precise()`,
			input:    1.5,
			expected: 2.0,
		},
		{
			name:     "precision 2 half_even",
			mapping:  `root = this.round_precise(precision: 2, rounding_style: "half_even")`,
			input:    2.455,
			expected: 2.46,
		},
		{
			name:     "ceil with precision",
			mapping:  `root = this.round_precise(precision: 1, rounding_style: "ceil")`,
			input:    1.31,
			expected: 1.4,
		},
		{
			name:     "floor negative number",
			mapping:  `root = this.round_precise(precision: 0, rounding_style: "floor")`,
			input:    -1.1,
			expected: -2.0,
		},
		{
			name:    "unknown rounding style errors",
			mapping: `root = this.round_precise(rounding_style: "banana")`,
			input:   1.5,
			wantErr: true,
		},
		{
			name:    "non-numeric input errors",
			mapping: `root = this.round_precise()`,
			input:   "hello",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ex, err := env.Parse(tc.mapping)
			require.NoError(t, err)

			result, err := ex.Query(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
