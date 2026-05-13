package util //nolint:revive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYAMLToMapNormalizesNestedMapTypes(t *testing.T) {
	fragmentMap, err := yamlToMap([]byte(`
limits_config:
  shard_streams:
    enabled: true
`))
	require.NoError(t, err)

	root, ok := fragmentMap.(map[interface{}]interface{})
	require.True(t, ok)
	limitsConfig, ok := root["limits_config"].(map[interface{}]interface{})
	require.True(t, ok)
	_, ok = limitsConfig["shard_streams"].(map[interface{}]interface{})
	require.True(t, ok)
}
