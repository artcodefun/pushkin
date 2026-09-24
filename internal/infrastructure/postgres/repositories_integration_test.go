//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	testpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/superman/pushkin/internal/application/ports"
	"github.com/superman/pushkin/internal/domain"
	"github.com/superman/pushkin/internal/infrastructure/postgres/gen"
	"github.com/superman/pushkin/internal/infrastructure/postgres/readrepo"
	"github.com/superman/pushkin/internal/infrastructure/postgres/repo"
)

func TestRepositoriesPersistConfigurationAndPushInstallation(t *testing.T) {
	pool := newIntegrationPool(t)
	queries := gen.New(pool)
	tenants := repo.NewTenantRepository(queries)
	providers := repo.NewProviderRepository(queries)
	channels := repo.NewChannelRepository(queries)
	applications := repo.NewMobileApplicationRepository(queries)
	installations := repo.NewPushInstallationRepository(queries)
	channelReads := readrepo.NewChannelRepository(queries)
	providerReads := readrepo.NewProviderRepository(queries)
	applicationReads := readrepo.NewMobileApplicationRepository(queries)
	ctx := context.Background()

	tenant, err := domain.NewTenant(domain.NewTenantParams{
		Name:               "integration-tenant",
		RateLimitPerMinute: 1000,
	})
	if err != nil {
		t.Fatalf("new tenant: %v", err)
	}
	if err := tenants.Create(ctx, tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID:             tenant.ID(),
		Type:                 domain.ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:         100,
		RateLimitBurst:       500,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	if err := providers.Create(ctx, provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}

	channel, err := domain.NewChannel(domain.NewChannelParams{
		TenantID: tenant.ID(),
		Provider: provider,
		Type:     domain.ChannelTypeMobilePush,
		Key:      "customer-app",
	})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if err := channels.Create(ctx, channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	transaction, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin channel provisioning transaction: %v", err)
	}
	provisioningChannel, err := channels.Tx(transaction).FindProvisioningForUpdate(ctx)
	if err != nil {
		_ = transaction.Rollback(ctx)
		t.Fatalf("find provisioning channel: %v", err)
	}
	if provisioningChannel.ID() != channel.ID() || provisioningChannel.Status() != domain.ChannelStatusProvisioning {
		_ = transaction.Rollback(ctx)
		t.Fatalf("unexpected provisioning channel: %+v", provisioningChannel)
	}
	if err := provisioningChannel.Activate(); err != nil {
		_ = transaction.Rollback(ctx)
		t.Fatalf("activate channel: %v", err)
	}
	if err := channels.Tx(transaction).Update(ctx, provisioningChannel); err != nil {
		_ = transaction.Rollback(ctx)
		t.Fatalf("update activated channel: %v", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatalf("commit channel provisioning transaction: %v", err)
	}
	activeChannelIDs, err := channels.ListActiveIDs(ctx)
	if err != nil {
		t.Fatalf("list active channel IDs: %v", err)
	}
	if len(activeChannelIDs) != 1 || activeChannelIDs[0] != channel.ID() {
		t.Fatalf("active channel IDs = %v, want [%s]", activeChannelIDs, channel.ID())
	}

	application, err := domain.NewMobileApplication(domain.NewMobileApplicationParams{
		TenantID:    tenant.ID(),
		Provider:    provider,
		Platform:    domain.MobilePlatformIOS,
		PackageName: "com.example.customer",
	})
	if err != nil {
		t.Fatalf("new mobile application: %v", err)
	}
	if err := applications.Create(ctx, application); err != nil {
		t.Fatalf("create mobile application: %v", err)
	}
	if err := channels.LinkMobileApplication(ctx, channel.ID(), application.ID()); err != nil {
		t.Fatalf("link mobile application: %v", err)
	}

	linkedIDs, err := applications.ListIDsByChannel(ctx, channel.ID())
	if err != nil {
		t.Fatalf("list channel applications: %v", err)
	}
	if len(linkedIDs) != 1 || linkedIDs[0] != application.ID() {
		t.Fatalf("linked application IDs = %v, want [%s]", linkedIDs, application.ID())
	}

	providerView, err := providerReads.FindByID(ctx, tenant.ID(), provider.ID())
	if err != nil {
		t.Fatalf("read provider: %v", err)
	}
	if providerView.RateLimitQPS != 100 || providerView.RateLimitBurst != 500 {
		t.Fatalf("unexpected provider read model: %+v", providerView)
	}
	channelView, err := channelReads.FindByID(ctx, tenant.ID(), channel.ID())
	if err != nil {
		t.Fatalf("read channel: %v", err)
	}
	if channelView.Key != channel.Key() || channelView.ProviderID != provider.ID() {
		t.Fatalf("unexpected channel read model: %+v", channelView)
	}
	applicationView, err := applicationReads.FindByID(ctx, tenant.ID(), application.ID())
	if err != nil {
		t.Fatalf("read mobile application: %v", err)
	}
	if applicationView.ProviderID == nil || *applicationView.ProviderID != provider.ID() {
		t.Fatalf("unexpected application provider: %+v", applicationView.ProviderID)
	}

	installation, err := domain.NewPushInstallation(domain.NewPushInstallationParams{
		TenantID:          tenant.ID(),
		UserID:            "subscriber-1",
		MobileApplication: application,
		InstallationID:    "installation-1",
		Token:             "token-v1",
	})
	if err != nil {
		t.Fatalf("new push installation: %v", err)
	}
	if err := installations.Upsert(ctx, installation); err != nil {
		t.Fatalf("upsert push installation: %v", err)
	}

	refreshed, err := domain.NewPushInstallation(domain.NewPushInstallationParams{
		TenantID:          tenant.ID(),
		UserID:            "subscriber-1",
		MobileApplication: application,
		InstallationID:    "installation-1",
		Token:             "token-v2",
	})
	if err != nil {
		t.Fatalf("new refreshed push installation: %v", err)
	}
	if err := installations.Upsert(ctx, refreshed); err != nil {
		t.Fatalf("refresh push installation: %v", err)
	}

	tokens, err := installations.ListActiveTokensByIDs(ctx, tenant.ID(), []domain.PushInstallationID{installation.ID()})
	if err != nil {
		t.Fatalf("list active tokens: %v", err)
	}
	if tokens[installation.ID()] != "token-v2" {
		t.Fatalf("active token = %q, want token-v2", tokens[installation.ID()])
	}
	if err := installations.Deactivate(ctx, tenant.ID(), installation.ID()); err != nil {
		t.Fatalf("deactivate installation: %v", err)
	}
	tokens, err = installations.ListActiveTokensByIDs(ctx, tenant.ID(), []domain.PushInstallationID{installation.ID()})
	if err != nil {
		t.Fatalf("list inactive token: %v", err)
	}
	if len(tokens) != 0 {
		t.Fatalf("inactive installation is still returned: %v", tokens)
	}

	if err := application.DisconnectProvider(); err != nil {
		t.Fatalf("disconnect provider: %v", err)
	}
	if err := applications.Update(ctx, application); err != nil {
		t.Fatalf("persist disconnected mobile application: %v", err)
	}
	applicationView, err = applicationReads.FindByID(ctx, tenant.ID(), application.ID())
	if err != nil {
		t.Fatalf("read disconnected mobile application: %v", err)
	}
	if applicationView.ProviderID != nil || applicationView.Status != domain.ConfigurationStatusDisabled {
		t.Fatalf("disconnected application was not persisted: %+v", applicationView)
	}
}

func TestCampaignAndSourceBatchRepositoriesShareTransaction(t *testing.T) {
	pool := newIntegrationPool(t)
	queries := gen.New(pool)
	ctx := context.Background()
	channel := createCampaignChannel(t, ctx, queries)
	campaigns := repo.NewCampaignRepository(queries)
	sourceBatches := repo.NewSourceBatchRepository(queries)
	transactions := repo.NewTransactionManager(pool)

	payload, err := domain.NewPushPayload("Balance reminder", "", "", map[string]string{"kind": "billing"})
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	campaign, err := domain.NewBatchedCampaign(domain.NewBatchedCampaignParams{
		TenantID:    channel.TenantID(),
		ChannelID:   channel.ID(),
		PushPayload: payload,
		Priority:    domain.PriorityNormal,
	})
	if err != nil {
		t.Fatalf("new campaign: %v", err)
	}
	batch, err := domain.NewSourceBatch(campaign.ID(), []domain.UserID{"subscriber-1", "subscriber-2"})
	if err != nil {
		t.Fatalf("new source batch: %v", err)
	}

	errRollback := errors.New("rollback integration transaction")
	err = transactions.WithTx(ctx, func(tx ports.Transaction) error {
		if err := campaigns.Tx(tx).Create(ctx, campaign); err != nil {
			return err
		}
		if err := sourceBatches.Tx(tx).Create(ctx, batch); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		t.Fatalf("rollback transaction error = %v, want %v", err, errRollback)
	}
	if _, err := campaigns.FindByID(ctx, campaign.ID()); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("rolled back campaign lookup error = %v, want not found", err)
	}

	if err := transactions.WithTx(ctx, func(tx ports.Transaction) error {
		if err := campaigns.Tx(tx).Create(ctx, campaign); err != nil {
			return err
		}
		return sourceBatches.Tx(tx).Create(ctx, batch)
	}); err != nil {
		t.Fatalf("commit campaign transaction: %v", err)
	}

	persistedCampaign, err := campaigns.FindByID(ctx, campaign.ID())
	if err != nil {
		t.Fatalf("find committed campaign: %v", err)
	}
	if persistedCampaign.PushPayload().Data()["kind"] != "billing" {
		t.Fatalf("campaign payload JSONB was not restored: %+v", persistedCampaign.PushPayload().Data())
	}
	persistedBatch, err := sourceBatches.FindByID(ctx, batch.ID())
	if err != nil {
		t.Fatalf("find committed source batch: %v", err)
	}
	if persistedBatch.Count() != 2 {
		t.Fatalf("source batch count = %d, want 2", persistedBatch.Count())
	}

	if err := campaign.RequestStart(); err != nil {
		t.Fatalf("request campaign start: %v", err)
	}
	runID, err := campaign.BeginRun(time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("begin campaign run: %v", err)
	}
	if err := transactions.WithTx(ctx, func(tx ports.Transaction) error {
		return campaigns.Tx(tx).UpdateBatch(ctx, []*domain.Campaign{campaign})
	}); err != nil {
		t.Fatalf("batch update campaign: %v", err)
	}

	persistedCampaign, err = campaigns.FindByID(ctx, campaign.ID())
	if err != nil {
		t.Fatalf("find batch-updated campaign: %v", err)
	}
	if persistedCampaign.Status() != domain.CampaignStatusStarting {
		t.Fatalf("campaign status = %q, want %q", persistedCampaign.Status(), domain.CampaignStatusStarting)
	}
	if persistedCampaign.RunID() == nil || *persistedCampaign.RunID() != runID {
		t.Fatalf("campaign run ID = %v, want %s", persistedCampaign.RunID(), runID)
	}
}

func TestCampaignRepositoryCreatePersistsInitialRun(t *testing.T) {
	pool := newIntegrationPool(t)
	queries := gen.New(pool)
	ctx := context.Background()
	channel := createCampaignChannel(t, ctx, queries)
	campaigns := repo.NewCampaignRepository(queries)

	payload, err := domain.NewPushPayload("Immediate", "", "", nil)
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	campaign, err := domain.NewInlineCampaign(domain.NewInlineCampaignParams{
		TenantID:    channel.TenantID(),
		ChannelID:   channel.ID(),
		PushPayload: payload,
		Priority:    domain.PriorityNormal,
		Recipients:  []domain.UserID{"subscriber-1"},
		Now:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("new inline campaign: %v", err)
	}
	runID, err := campaign.BeginRun(time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("begin campaign run: %v", err)
	}
	if err := campaigns.Create(ctx, campaign); err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	persisted, err := campaigns.FindByID(ctx, campaign.ID())
	if err != nil {
		t.Fatalf("find campaign: %v", err)
	}
	if persisted.RunID() == nil || *persisted.RunID() != runID {
		t.Fatalf("persisted run ID = %v, want %s", persisted.RunID(), runID)
	}
	if persisted.RunAttemptedAt() == nil {
		t.Fatal("persisted run attempted at is nil")
	}
}

func TestCampaignRepositoryPersistsInlineRecipients(t *testing.T) {
	pool := newIntegrationPool(t)
	queries := gen.New(pool)
	ctx := context.Background()
	channel := createCampaignChannel(t, ctx, queries)
	campaigns := repo.NewCampaignRepository(queries)
	payload, err := domain.NewPushPayload("Inline", "", "", nil)
	if err != nil {
		t.Fatalf("new push payload: %v", err)
	}
	scheduledAt := time.Now().UTC().Add(time.Hour)
	campaign, err := domain.NewInlineCampaign(domain.NewInlineCampaignParams{
		TenantID: channel.TenantID(), ChannelID: channel.ID(), PushPayload: payload, Priority: domain.PriorityHigh,
		Recipients: []domain.UserID{"user-1", "user-2"}, ScheduledAt: &scheduledAt, Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("new inline campaign: %v", err)
	}
	if err := campaigns.Create(ctx, campaign); err != nil {
		t.Fatalf("create inline campaign: %v", err)
	}
	stored, err := campaigns.FindByID(ctx, campaign.ID())
	if err != nil {
		t.Fatalf("find inline campaign: %v", err)
	}
	if stored.RecipientMode() != domain.CampaignRecipientModeInline {
		t.Fatalf("recipient mode = %q", stored.RecipientMode())
	}
	if recipients := stored.InlineRecipients(); len(recipients) != 2 || recipients[0] != "user-1" || recipients[1] != "user-2" {
		t.Fatalf("unexpected inline recipients: %v", recipients)
	}
	if stored.ScheduledAt() == nil || !stored.ScheduledAt().Equal(scheduledAt) {
		t.Fatalf("scheduled at = %v, want %v", stored.ScheduledAt(), scheduledAt)
	}
}

func createCampaignChannel(t *testing.T, ctx context.Context, queries *gen.Queries) *domain.Channel {
	t.Helper()
	tenants := repo.NewTenantRepository(queries)
	providers := repo.NewProviderRepository(queries)
	channels := repo.NewChannelRepository(queries)
	tenant, err := domain.NewTenant(domain.NewTenantParams{
		Name:               "campaign-tenant",
		RateLimitPerMinute: 1000,
	})
	if err != nil {
		t.Fatalf("new tenant: %v", err)
	}
	if err := tenants.Create(ctx, tenant); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	provider, err := domain.NewProvider(domain.NewProviderParams{
		TenantID:             tenant.ID(),
		Type:                 domain.ProviderTypeFCM,
		EncryptedCredentials: "encrypted-credentials",
		RateLimitQPS:         100,
		RateLimitBurst:       500,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	if err := providers.Create(ctx, provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	channel, err := domain.NewChannel(domain.NewChannelParams{
		TenantID: tenant.ID(),
		Provider: provider,
		Type:     domain.ChannelTypeMobilePush,
		Key:      "customer-app",
	})
	if err != nil {
		t.Fatalf("new channel: %v", err)
	}
	if err := channels.Create(ctx, channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	return channel
}

func newIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), startPostgresContainer(t))
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	applyMigration(t, pool)
	return pool
}

func startPostgresContainer(t *testing.T) string {
	t.Helper()
	container, err := testpostgres.Run(
		context.Background(),
		"postgres:17-alpine",
		testpostgres.WithDatabase("pushkin_test"),
		testpostgres.WithUsername("pushkin_test"),
		testpostgres.WithPassword("pushkin_test"),
		testpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start PostgreSQL test container: %v", err)
	}
	dsn, err := container.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		t.Fatalf("get PostgreSQL test container connection string: %v", err)
	}
	return dsn
}

func applyMigration(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	migrationsPath := filepath.Join(filepath.Dir(file), "migrations")
	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		migrationPath := filepath.Join(migrationsPath, entry.Name())
		migration, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		if _, err := pool.Exec(context.Background(), string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}
}
