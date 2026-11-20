package api

import _ "embed"

// OpenAPISpec contains the embedded OpenAPI 3.0.3 YAML specification.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
