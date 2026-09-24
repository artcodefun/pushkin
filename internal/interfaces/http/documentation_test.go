package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	contract "github.com/superman/pushkin/api/http/v1"
)

func TestDocumentationRoutesArePublic(t *testing.T) {
	t.Setenv("GIN_MODE", gin.TestMode)
	router := NewRouter(Commands{}, Queries{}, nil, "")

	openAPIRequest := httptest.NewRequest(http.MethodGet, "/api/v1/openapi.yaml", nil)
	openAPIResponse := httptest.NewRecorder()
	router.ServeHTTP(openAPIResponse, openAPIRequest)
	if openAPIResponse.Code != http.StatusOK {
		t.Fatalf("openapi status = %d, want %d", openAPIResponse.Code, http.StatusOK)
	}
	if got, want := openAPIResponse.Header().Get("Content-Type"), "application/yaml; charset=utf-8"; got != want {
		t.Fatalf("openapi content type = %q, want %q", got, want)
	}
	if got, want := openAPIResponse.Body.String(), string(contract.OpenAPI()); got != want {
		t.Fatal("openapi response differs from embedded contract")
	}
	if !strings.Contains(openAPIResponse.Body.String(), "servers:\n  - url: /api/v1") {
		t.Fatal("openapi server must define the API version prefix")
	}

	docsRequest := httptest.NewRequest(http.MethodGet, "/api/v1/docs/", nil)
	docsResponse := httptest.NewRecorder()
	router.ServeHTTP(docsResponse, docsRequest)
	if docsResponse.Code != http.StatusOK {
		t.Fatalf("docs status = %d, want %d", docsResponse.Code, http.StatusOK)
	}
	if got, want := docsResponse.Header().Get("Content-Type"), "text/html; charset=utf-8"; got != want {
		t.Fatalf("docs content type = %q, want %q", got, want)
	}
	if !strings.Contains(docsResponse.Body.String(), "SwaggerUIBundle") {
		t.Fatal("docs response does not contain Swagger UI bootstrap")
	}
}
