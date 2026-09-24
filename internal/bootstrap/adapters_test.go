package bootstrap

import (
	"net/http"
	"testing"
	"time"
)

func TestNewPushHTTPClientSizesConnectionPoolToConcurrencyLimit(t *testing.T) {
	t.Parallel()

	client := newPushHTTPClient(500)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if client.Timeout != 30*time.Second {
		t.Fatalf("timeout = %s, want %s", client.Timeout, 30*time.Second)
	}
	if transport.MaxIdleConns != 500 || transport.MaxIdleConnsPerHost != 500 || transport.MaxConnsPerHost != 500 {
		t.Fatalf("unexpected pool limits: total=%d host=%d max=%d", transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost)
	}
}
