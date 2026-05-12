package client

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigRegisterFlagsWithPrefix(t *testing.T) {
	cfg := Config{}
	f := flag.NewFlagSet(t.Name(), flag.PanicOnError)

	cfg.RegisterFlagsWithPrefix("rate-service-client", f)

	require.NotNil(t, f.Lookup("rate-service-client.address"))
	require.Nil(t, f.Lookup("address"))
}
