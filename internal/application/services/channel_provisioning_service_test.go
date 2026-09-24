package services

import (
	"context"
	"testing"

	"uuid"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
)

func TestChannelProvisioningServiceProvisionsAndActivatesChannel(t *testing.T) {
	t.Parallel()

	channel := newProvisioningChannel(t)
	repository := &channelProvisioningRepository{channel: channel}
	provisioner := &channelTopicProvisioner{}
	service := NewChannelProvisioningService(ChannelProvisioningServiceParams{
		ChannelRepository:       repository,
		TransactionManager:      channelProvisioningTransactionManager{},
		ChannelTopicProvisioner: provisioner,
	})

	processed, err := service.Process(context.Background())
	if err != nil {
		t.Fatalf("process channel provisioning: %v", err)
	}
	if !processed {
		t.Fatal("expected a channel to be provisioned")
	}
	if provisioner.channelID != channel.ID() {
		t.Fatalf("provisioned channel = %s, want %s", provisioner.channelID, channel.ID())
	}
	if channel.Status() != domain.ChannelStatusActive {
		t.Fatalf("channel status = %q, want active", channel.Status())
	}
	if repository.updated != channel {
		t.Fatal("expected activated channel to be persisted")
	}
}

func TestChannelProvisioningServiceSkipsWhenNoUnlockedChannelExists(t *testing.T) {
	t.Parallel()

	service := NewChannelProvisioningService(ChannelProvisioningServiceParams{
		ChannelRepository:       &channelProvisioningRepository{},
		TransactionManager:      channelProvisioningTransactionManager{},
		ChannelTopicProvisioner: &channelTopicProvisioner{},
	})

	processed, err := service.Process(context.Background())
	if err != nil {
		t.Fatalf("process channel provisioning: %v", err)
	}
	if processed {
		t.Fatal("expected no channel to be provisioned")
	}
}

type channelProvisioningRepository struct {
	ports.ChannelRepository
	channel *domain.Channel
	updated *domain.Channel
}

func (r *channelProvisioningRepository) FindProvisioningForUpdate(context.Context) (*domain.Channel, error) {
	if r.channel == nil {
		return nil, ports.ErrNotFound
	}
	return r.channel, nil
}

func (r *channelProvisioningRepository) Update(_ context.Context, channel *domain.Channel) error {
	r.updated = channel
	return nil
}

func (r *channelProvisioningRepository) Tx(ports.Transaction) ports.ChannelRepository {
	return r
}

type channelProvisioningTransactionManager struct{}

func (channelProvisioningTransactionManager) WithTx(_ context.Context, fn func(ports.Transaction) error) error {
	return fn(struct{}{})
}

type channelTopicProvisioner struct{ channelID domain.ChannelID }

func (p *channelTopicProvisioner) EnsureChannelTopics(_ context.Context, channelID domain.ChannelID) error {
	p.channelID = channelID
	return nil
}

func newProvisioningChannel(t *testing.T) *domain.Channel {
	t.Helper()
	tenantID := uuid.NewV7()
	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID:             tenantID,
		Type:                 domain.ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:         1,
		RateLimitBurst:       1,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	channel, err := domain.NewChannel(domain.NewChannelParams{
		TenantID: tenantID,
		Provider: provider,
		Type:     domain.ChannelTypeMobilePush,
		Key:      "channel",
	})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	return channel
}

var _ ports.ChannelRepository = (*channelProvisioningRepository)(nil)
var _ ports.ChannelTopicProvisioner = (*channelTopicProvisioner)(nil)
var _ ports.TransactionManager = channelProvisioningTransactionManager{}
