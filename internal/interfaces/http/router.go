package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/interfaces/http/handlers"
	"github.com/superman/pushkin/internal/interfaces/http/middleware"
)

type Commands struct {
	Campaign          application.CampaignCommands
	Provider          application.ProviderCommands
	MobileApplication application.MobileApplicationCommands
	Channel           application.ChannelCommands
	Tenant            application.TenantCommands
	TenantAPIKey      application.TenantAPIKeyCommands
	PushInstallation  application.PushInstallationCommands
	Notification      application.NotificationCommands
}

type Queries struct {
	Campaign          application.CampaignQueries
	Tenant            application.TenantQueries
	Provider          application.ProviderQueries
	MobileApplication application.MobileApplicationQueries
	Channel           application.ChannelQueries
	Notification      application.NotificationQueries
}

func NewRouter(commands Commands, queries Queries, validator ports.TenantAPIKeyValidator, adminMasterKey string) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(newHTTPMetrics().observe())
	router.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })
	registerDocumentationRoutes(router)

	internal := router.Group("/api/v1/internal", middleware.RequireMasterKey(adminMasterKey))
	tenants := handlers.NewTenantHandler(commands.Tenant, queries.Tenant)
	internal.POST("/tenants", tenants.Create)
	internal.GET("/tenants", tenants.List)

	tenantAPIKeys := handlers.NewTenantAPIKeyHandler(commands.TenantAPIKey)
	internal.POST("/tenant-api-keys", tenantAPIKeys.Issue)
	internal.DELETE("/tenant-api-keys/:api_key_id", tenantAPIKeys.Revoke)

	campaigns := handlers.NewCampaignHandler(commands.Campaign, queries.Campaign)
	api := router.Group("/api/v1", middleware.RequireTenantAPIKey(validator))
	api.POST("/campaigns", campaigns.Create)
	api.POST("/campaigns:inline", campaigns.CreateInline)
	api.GET("/campaigns/:campaign_id", campaigns.Get)
	api.POST("/campaigns/:campaign_id/recipients:batch", campaigns.AddRecipients)
	api.POST("/campaigns/:campaign_id/start", campaigns.Start)

	installations := handlers.NewPushInstallationHandler(commands.PushInstallation)
	api.POST("/push-installations:register", installations.Register)

	notifications := handlers.NewNotificationHandler(commands.Notification, queries.Notification)
	api.GET("/users/:user_id/notifications", notifications.List)
	api.POST("/users/:user_id/notifications/:notification_id/read", notifications.MarkRead)

	providers := handlers.NewProviderHandler(commands.Provider, queries.Provider)
	api.POST("/providers", providers.Create)
	api.GET("/providers", providers.List)
	api.GET("/providers/:provider_id", providers.Get)

	mobileApplications := handlers.NewMobileApplicationHandler(commands.MobileApplication, queries.MobileApplication)
	api.POST("/mobile-applications", mobileApplications.Create)
	api.GET("/mobile-applications", mobileApplications.List)
	api.GET("/mobile-applications/:mobile_application_id", mobileApplications.Get)
	api.PUT("/mobile-applications/:mobile_application_id/provider", mobileApplications.ConnectProvider)
	api.DELETE("/mobile-applications/:mobile_application_id/provider", mobileApplications.DisconnectProvider)

	channels := handlers.NewChannelHandler(commands.Channel, queries.Channel)
	api.POST("/channels", channels.Create)
	api.GET("/channels", channels.List)
	api.GET("/channels/:channel_id", channels.Get)
	api.PUT("/channels/:channel_id/mobile-applications/:mobile_application_id", channels.LinkMobileApplication)
	api.DELETE("/channels/:channel_id/mobile-applications/:mobile_application_id", channels.UnlinkMobileApplication)
	return router
}
