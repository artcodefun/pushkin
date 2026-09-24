package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	contract "github.com/superman/pushkin/api/http/v1"
)

const swaggerUI = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Pushkin API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    SwaggerUIBundle({
      url: "../openapi.yaml",
      dom_id: "#swagger-ui",
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout"
    });
  </script>
</body>
</html>`

func registerDocumentationRoutes(router *gin.Engine) {
	router.GET("/api/v1/openapi.yaml", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/yaml; charset=utf-8", contract.OpenAPI())
	})
	router.GET("/api/v1/docs", func(c *gin.Context) {
		c.Redirect(http.StatusPermanentRedirect, "/api/v1/docs/")
	})
	router.GET("/api/v1/docs/*path", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(swaggerUI))
	})
}
