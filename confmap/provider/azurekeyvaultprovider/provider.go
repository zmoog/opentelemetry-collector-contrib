// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate make mdatagen

package azurekeyvaultprovider // import "github.com/open-telemetry/opentelemetry-collector-contrib/confmap/provider/azurekeyvaultprovider"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
)

type keyVaultClient interface {
	GetSecret(ctx context.Context, name string, version string, options *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error)
}

const (
	schemeName = "azurekeyvault"
)

var (
	ErrURINotSupported = errors.New("uri is not supported by Azure Key Vault Provider")
	ErrInvalidURI      = errors.New("invalid Azure Key Vault secret URI")
	ErrGetSecret       = errors.New("failed to get secret from Azure Key Vault")
)

type provider struct {
	client keyVaultClient
	logger *zap.Logger
}

// NewFactory returns a new confmap.ProviderFactory that creates a confmap.Provider
// which reads configuration from Azure Key Vault.
//
// This Provider supports "azurekeyvault" scheme, and can be called with a selector:
// `azurekeyvault:https://{vault-name}.vault.azure.net/secrets/{secret-name}[/{version}]`
//
// A JSON key can be extracted using # separator:
// `azurekeyvault:https://{vault-name}.vault.azure.net/secrets/{secret-name}#json-key`
//
// A default value for unset variable can be provided after :- suffix:
// `azurekeyvault:https://{vault-name}.vault.azure.net/secrets/{secret-name}:-default_value`
func NewFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(newWithSettings)
}

func newWithSettings(ps confmap.ProviderSettings) confmap.Provider {
	return &provider{client: nil, logger: ps.Logger}
}

func (p *provider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+":") {
		return nil, fmt.Errorf("%q: %w", uri, ErrURINotSupported)
	}

	spec := strings.TrimPrefix(uri, schemeName+":")
	// split by :- to get the default value
	selector, defaultValue, hasDefaultValue := strings.Cut(spec, ":-")
	// split by # to get the json key
	selector, secretJSONKey, jsonKeyFound := strings.Cut(selector, "#")

	// Parse the Azure Key Vault secret URL
	vaultURL, secretName, secretVersion, err := parseSecretURL(selector)
	if err != nil {
		if hasDefaultValue {
			p.logger.Warn("Azure Key Vault selector invalid, falling back to default value")
			return confmap.NewRetrieved(defaultValue)
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidURI, err)
	}

	if secretName == "" && hasDefaultValue {
		p.logger.Warn("Azure Key Vault secret name empty, falling back to default value")
		return confmap.NewRetrieved(defaultValue)
	}

	// initialize the Azure Key Vault client on first call
	if p.client == nil {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure default credential: %w", err)
		}
		client, err := azsecrets.NewClient(vaultURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Key Vault client: %w", err)
		}
		p.client = client
	}

	response, err := p.client.GetSecret(ctx, secretName, secretVersion, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrGetSecret, err)
	}

	if response.Value == nil {
		return nil, nil
	}

	secretValue := *response.Value

	if jsonKeyFound {
		var secretFieldsMap map[string]any
		err := json.Unmarshal([]byte(secretValue), &secretFieldsMap)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling secret string: %w", err)
		}

		fieldValue, ok := secretFieldsMap[secretJSONKey]
		if !ok {
			if hasDefaultValue {
				p.logger.Warn("field not found in secret map, falling back to default value")
				return confmap.NewRetrieved(defaultValue)
			}
			return nil, fmt.Errorf("field %q not found in secret map", secretJSONKey)
		}

		return confmap.NewRetrieved(fieldValue)
	}

	return confmap.NewRetrieved(secretValue)
}

func (*provider) Scheme() string {
	return schemeName
}

func (*provider) Shutdown(context.Context) error {
	return nil
}

// parseSecretURL parses an Azure Key Vault secret URL and extracts the vault URL,
// secret name, and optional version.
// Expected format: https://{vault-name}.vault.azure.net/secrets/{secret-name}[/{version}]
func parseSecretURL(rawURL string) (vaultURL, secretName, secretVersion string, err error) {
	if rawURL == "" {
		return "", "", "", fmt.Errorf("empty secret URL")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to parse URL %q: %w", rawURL, err)
	}

	vaultURL = u.Scheme + "://" + u.Host

	// Path should be /secrets/{name}[/{version}]
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "secrets" {
		return "", "", "", fmt.Errorf("URL path must follow /secrets/{name}[/{version}] format, got %q", u.Path)
	}

	secretName = parts[1]
	if len(parts) >= 3 {
		secretVersion = parts[2]
	}

	return vaultURL, secretName, secretVersion, nil
}
