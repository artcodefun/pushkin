package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

func RequireMasterKey(expected string) gin.HandlerFunc {
	return func(c *gin.Context) {
		provided := c.GetHeader("X-Pushkin-Admin-Key")
		if expected == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}
