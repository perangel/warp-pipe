package warppipe

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSemver(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		major int
		minor int
	}{
		{raw: "9.5", major: 9, minor: 5},
		{raw: "9.5   (And some other stuff...", major: 9, minor: 5},
		{raw: "9.5.1  (patch version ignored)", major: 9, minor: 5},
		{raw: "12.34  (multi-digit check)", major: 12, minor: 34},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			major, minor, err := parseSemver(tc.raw)
			assert.NoError(t, err)
			assert.Equal(t, tc.major, major)
			assert.Equal(t, tc.minor, minor)
		})
	}
}
