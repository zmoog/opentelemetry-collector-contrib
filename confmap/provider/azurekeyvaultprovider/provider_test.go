// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azurekeyvaultprovider

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
)

// Mock Azure Key Vault client
type mockKeyVaultClient struct {
	secrets map[string]map[string]string // name -> version -> value ("" version = latest)
}

func (m *mockKeyVaultClient) GetSecret(_ context.Context, name string, version string, _ *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error) {
	versions, ok := m.secrets[name]
	if !ok {
		return azsecrets.GetSecretResponse{}, fmt.Errorf("secret %q not found", name)
	}
	value, ok := versions[version]
	if !ok {
		return azsecrets.GetSecretResponse{}, fmt.Errorf("secret %q version %q not found", name, version)
	}
	return azsecrets.GetSecretResponse{
		Secret: azsecrets.Secret{
			Value: &value,
		},
	}, nil
}

func newTestProvider(secrets map[string]map[string]string) confmap.Provider {
	return &provider{
		client: &mockKeyVaultClient{secrets: secrets},
		logger: zap.NewNop(),
	}
}

func simpleSecrets(name, value string) map[string]map[string]string {
	return map[string]map[string]string{
		name: {"": value},
	}
}

func TestRetrieveSuccess(t *testing.T) {
	fp := newTestProvider(simpleSecrets("my-secret", "secret-value"))
	result, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret", nil)

	require.NoError(t, err)
	assert.NoError(t, fp.Shutdown(t.Context()))

	value, err := result.AsRaw()
	require.NoError(t, err)
	assert.Equal(t, "secret-value", value)
}

func TestRetrieveSuccessWithVersion(t *testing.T) {
	secrets := map[string]map[string]string{
		"my-secret": {"v1": "versioned-value"},
	}
	fp := newTestProvider(secrets)
	result, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret/v1", nil)

	require.NoError(t, err)
	assert.NoError(t, fp.Shutdown(t.Context()))

	value, err := result.AsRaw()
	require.NoError(t, err)
	assert.Equal(t, "versioned-value", value)
}

func TestRetrieveIgnoreDefault(t *testing.T) {
	fp := newTestProvider(simpleSecrets("my-secret", "real-value"))
	result, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret:-default", nil)

	require.NoError(t, err)

	value, err := result.AsRaw()
	require.NoError(t, err)
	assert.Equal(t, "real-value", value)
}

func TestRetrieveJSONKeyValid(t *testing.T) {
	secretJSON := `{"field1": "extracted-value"}`
	fp := newTestProvider(simpleSecrets("my-secret", secretJSON))
	result, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret#field1", nil)

	require.NoError(t, err)

	value, err := result.AsRaw()
	require.NoError(t, err)
	assert.Equal(t, "extracted-value", value)
}

func TestRetrieveJSONKeyInvalid(t *testing.T) {
	fp := newTestProvider(simpleSecrets("my-secret", "not-json"))
	_, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret#field1", nil)

	assert.Error(t, err)
}

func TestRetrieveJSONKeyMissing(t *testing.T) {
	secretJSON := `{"field0": "some-value"}`
	fp := newTestProvider(simpleSecrets("my-secret", secretJSON))
	_, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret#field1", nil)

	assert.Error(t, err)
}

func TestRetrieveJSONKeyMissingWithDefault(t *testing.T) {
	secretJSON := `{"field0": "some-value"}`
	fp := newTestProvider(simpleSecrets("my-secret", secretJSON))
	result, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret#field1:-fallback", nil)

	require.NoError(t, err)

	value, err := result.AsRaw()
	require.NoError(t, err)
	assert.Equal(t, "fallback", value)
}

func TestRetrieveInvalidScheme(t *testing.T) {
	fp := newTestProvider(simpleSecrets("my-secret", "value"))
	_, err := fp.Retrieve(t.Context(), "invalid:https://myvault.vault.azure.net/secrets/my-secret", nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrURINotSupported)
}

func TestRetrieveSecretNotFound(t *testing.T) {
	fp := newTestProvider(simpleSecrets("other-secret", "value"))
	_, err := fp.Retrieve(t.Context(), "azurekeyvault:https://myvault.vault.azure.net/secrets/my-secret", nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrGetSecret)
}

func TestRetrieveDefaultValueInvalidURL(t *testing.T) {
	fp := newTestProvider(simpleSecrets("my-secret", "value"))
	result, err := fp.Retrieve(t.Context(), "azurekeyvault::-defaultValue", nil)

	require.NoError(t, err)

	value, err := result.AsRaw()
	require.NoError(t, err)
	assert.Equal(t, "defaultValue", value)
}

func TestFactory(t *testing.T) {
	p := NewFactory().Create(confmap.ProviderSettings{})
	_, ok := p.(*provider)
	require.True(t, ok)
}

func TestScheme(t *testing.T) {
	p := NewFactory().Create(confmap.ProviderSettings{})
	assert.Equal(t, "azurekeyvault", p.Scheme())
}

func TestParseSecretURL(t *testing.T) {
	tests := []struct {
		name          string
		rawURL        string
		wantVaultURL  string
		wantName      string
		wantVersion   string
		wantErr       bool
	}{
		{
			name:         "full URL with version",
			rawURL:       "https://myvault.vault.azure.net/secrets/my-secret/v1",
			wantVaultURL: "https://myvault.vault.azure.net",
			wantName:     "my-secret",
			wantVersion:  "v1",
		},
		{
			name:         "URL without version",
			rawURL:       "https://myvault.vault.azure.net/secrets/my-secret",
			wantVaultURL: "https://myvault.vault.azure.net",
			wantName:     "my-secret",
			wantVersion:  "",
		},
		{
			name:    "empty URL",
			rawURL:  "",
			wantErr: true,
		},
		{
			name:    "invalid path format",
			rawURL:  "https://myvault.vault.azure.net/keys/my-key",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vaultURL, secretName, secretVersion, err := parseSecretURL(tc.rawURL)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantVaultURL, vaultURL)
			assert.Equal(t, tc.wantName, secretName)
			assert.Equal(t, tc.wantVersion, secretVersion)
		})
	}
}
