package bloblang

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/encoding/charmap"
	"testing"
)

func TestEbcdicToUtf8(t *testing.T) {
	// Test cases with known EBCDIC to UTF-8 conversions
	testCases := []struct {
		name      string
		input     []byte
		expected  []byte
		shouldErr bool
	}{
		{
			name:      "empty input",
			input:     []byte{},
			expected:  []byte{},
			shouldErr: false,
		},
		{
			name:      "simple ASCII text (EBCDIC encoded)",
			input:     []byte{0xC1, 0xC2, 0xC3}, // 'A', 'B', 'C' in EBCDIC
			expected:  []byte("ABC"),
			shouldErr: false,
		},
		{
			name:      "numbers in EBCDIC",
			input:     []byte{0xF0, 0xF1, 0xF2}, // '0', '1', '2' in EBCDIC
			expected:  []byte("012"),
			shouldErr: false,
		},
		{
			name:      "mixed case text",
			input:     []byte{0xC1, 0x82, 0x83}, // 'A', 'b', 'c' in EBCDIC
			expected:  []byte("Abc"),
			shouldErr: false,
		},
		{
			name:      "special characters",
			input:     []byte{0x4B, 0x6B, 0x60}, // '.', ',', '-' in EBCDIC
			expected:  []byte(".,-"),
			shouldErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ebcdic_to_utf8(tc.input)

			if tc.shouldErr {
				assert.Error(t, err, "Expected error but got none")
			} else {
				assert.NoError(t, err, "Unexpected error: %v", err)
				assert.Equal(t, tc.expected, result, "Conversion result mismatch")
			}
		})
	}
}

func TestEbcdicToUtf8WithRealDecoder(t *testing.T) {
	// Test that our function produces the same result as the direct decoder
	testInput := []byte{0xC1, 0xC2, 0xC3, 0x40, 0x5A} // 'A', 'B', 'C', ' ', '.' in EBCDIC

	result, err := ebcdic_to_utf8(testInput)
	assert.NoError(t, err)

	// Compare with direct decoder
	decoder := charmap.CodePage1047.NewDecoder()
	expected, err := decoder.Bytes(testInput)
	assert.NoError(t, err)

	assert.Equal(t, expected, result, "Our function should produce same result as direct decoder")
}

func TestEbcdicToUtf8ErrorHandling(t *testing.T) {
	// Test with invalid EBCDIC data
	invalidInput := []byte{0x00, 0x01, 0x02} // Invalid EBCDIC sequence

	_, err := ebcdic_to_utf8(invalidInput)
	// The decoder might handle this gracefully or return an error
	// We just want to ensure it doesn't panic
	if err != nil {
		t.Logf("Got expected error for invalid input: %v", err)
	} else {
		t.Log("Decoder handled invalid input gracefully")
	}
}
