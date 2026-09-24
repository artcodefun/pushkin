package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

const tenantIDContextKey = "tenant_id"

func RequireTenantAPIKey(validator ports.TenantAPIKeyValidator) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := strings.TrimSpace(c.GetHeader("X-Pushkin-API-Key"))
		tenantID, err := validator.Validate(c.Request.Context(), key)
		if err != nil {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Set(tenantIDContextKey, tenantID)
		c.Next()
	}
}

func TenantID(c *gin.Context) domain.TenantID {
	value, _ := c.Get(tenantIDContextKey)
	tenantID, _ := value.(domain.TenantID)
	return tenantID
}
