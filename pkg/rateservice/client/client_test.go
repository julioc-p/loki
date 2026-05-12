package client

import (
	"testing"

	"github.com/grafana/dskit/grpcclient"
	"github.com/stretchr/testify/require"
)

func TestNewClientAppliesGRPCDialOptions(t *testing.T) {
	grpcConfig := grpcclient.Config{
		TLSEnabled: true,
	}
	grpcConfig.TLS.CertPath = "missing-cert"

	_, err := NewClient(Config{
		Address:          "localhost:9095",
		GRPCClientConfig: grpcConfig,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate given but no key configured")
}
