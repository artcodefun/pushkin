package http

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type httpMetrics struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
}

func newHTTPMetrics() httpMetrics {
	meter := otel.Meter("github.com/superman/pushkin/http")
	requests, _ := meter.Int64Counter(
		"pushkin.http.server.requests",
		metric.WithDescription("Number of completed HTTP requests."),
	)
	duration, _ := meter.Float64Histogram(
		"pushkin.http.server.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of completed HTTP requests."),
	)
	return httpMetrics{requests: requests, duration: duration}
}

func (m httpMetrics) observe() gin.HandlerFunc {
	return func(c *gin.Context) {
		startedAt := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		attributes := metric.WithAttributes(
			attribute.String("http.request.method", c.Request.Method),
			attribute.String("http.route", route),
			attribute.Int("http.response.status_code", c.Writer.Status()),
		)
		m.requests.Add(c.Request.Context(), 1, attributes)
		m.duration.Record(c.Request.Context(), time.Since(startedAt).Seconds(), attributes)
	}
}
