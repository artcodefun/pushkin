package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/superman/pushkin/internal/application/ports"
)

// ChannelProvisioningService creates a Channel's dynamic Kafka topics before
// making that Channel available for Campaigns. It holds a row lock while
// provisioning so only one Pushkin instance configures a Channel at a time.
type ChannelProvisioningService struct {
	channels     ports.ChannelRepository
	transactions ports.TransactionManager
	provisioner  ports.ChannelTopicProvisioner
}

type ChannelProvisioningServiceParams struct {
	ChannelRepository       ports.ChannelRepository
	TransactionManager      ports.TransactionManager
	ChannelTopicProvisioner ports.ChannelTopicProvisioner
}

func NewChannelProvisioningService(params ChannelProvisioningServiceParams) *ChannelProvisioningService {
	return &ChannelProvisioningService{
		channels:     params.ChannelRepository,
		transactions: params.TransactionManager,
		provisioner:  params.ChannelTopicProvisioner,
	}
}

// Process provisions at most one Channel. processed is false when no unlocked
// provisioning Channel currently exists.
func (s *ChannelProvisioningService) Process(ctx context.Context) (processed bool, err error) {
	err = s.transactions.WithTx(ctx, func(tx ports.Transaction) error {
		channels := s.channels.Tx(tx)
		channel, err := channels.FindProvisioningForUpdate(ctx)
		if errors.Is(err, ports.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("find provisioning channel: %w", err)
		}

		if err := s.provisioner.EnsureChannelTopics(ctx, channel.ID()); err != nil {
			return fmt.Errorf("ensure channel Kafka topics: %w", err)
		}
		if err := channel.Activate(); err != nil {
			return err
		}
		if err := channels.Update(ctx, channel); err != nil {
			return fmt.Errorf("activate channel: %w", err)
		}
		processed = true
		return nil
	})
	return processed, err
}
