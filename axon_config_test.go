package warppipe

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAxonConfigListenerMode(t *testing.T) {
	envVarListenerMode := "AXON_LISTENER_MODE"

	for _, tc := range []struct {
		name       string
		vars       map[string]string
		expectMode string
		expectErr  bool
	}{
		{
			name:       "default (notify)",
			expectMode: ListenerModeNotify,
		},
		{
			name: "specify notify",
			vars: map[string]string{
				envVarListenerMode: ListenerModeNotify,
			},
			expectMode: ListenerModeNotify,
		},
		{
			name: "specify replication",
			vars: map[string]string{
				envVarListenerMode: ListenerModeLogicalReplication,
			},
			expectMode: ListenerModeLogicalReplication,
		},
		{
			name: "bad mode",
			vars: map[string]string{
				envVarListenerMode: "garbage",
			},
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for key, val := range tc.vars {
				assert.NoError(t, os.Setenv(key, val))
				defer os.Unsetenv(key)
			}

			config, err := NewAxonConfigFromEnv()

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectMode, config.ListenerMode)
			}
		})
	}
}
