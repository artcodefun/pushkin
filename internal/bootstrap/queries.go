package bootstrap

import (
	"github.com/superman/pushkin/internal/application"
	"github.com/superman/pushkin/internal/application/queries"
)

// Queries contains the read use cases exposed by primary adapters.
type Queries struct {
	Campaign          application.CampaignQueries
	Tenant            application.TenantQueries
	Channel           application.ChannelQueries
	MobileApplication application.MobileApplicationQueries
	Provider          application.ProviderQueries
	Notification      application.NotificationQueries
}

func NewQueries(adapters *Adapters) *Queries {
	return &Queries{
		Campaign:          queries.NewCampaignQueries(adapters.CampaignReads),
		Tenant:            queries.NewTenantQueries(adapters.TenantReads),
		Channel:           queries.NewChannelQueries(adapters.ChannelReads),
		MobileApplication: queries.NewMobileApplicationQueries(adapters.MobileApplicationReads),
		Provider:          queries.NewProviderQueries(adapters.ProviderReads),
		Notification:      queries.NewNotificationQueries(adapters.NotificationReads),
	}
}
