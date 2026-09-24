CREATE TABLE tenants (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    rate_limit_per_minute BIGINT NOT NULL CHECK (rate_limit_per_minute > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE providers (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    provider_type TEXT NOT NULL,
    encrypted_credentials TEXT NOT NULL,
    rate_limit_qps BIGINT NOT NULL CHECK (rate_limit_qps > 0),
    rate_limit_burst BIGINT NOT NULL CHECK (rate_limit_burst > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX providers_tenant_id_idx ON providers (tenant_id);

CREATE TABLE channels (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    provider_id UUID NOT NULL REFERENCES providers(id),
    channel_type TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_key)
);

CREATE INDEX channels_tenant_id_idx ON channels (tenant_id);

CREATE TABLE mobile_applications (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    provider_id UUID REFERENCES providers(id),
    platform TEXT NOT NULL CHECK (platform IN ('android', 'ios')),
    package_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, platform, package_name)
);

CREATE INDEX mobile_applications_tenant_id_idx ON mobile_applications (tenant_id);
CREATE INDEX mobile_applications_provider_id_idx ON mobile_applications (provider_id);

CREATE TABLE channel_mobile_applications (
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    mobile_application_id UUID NOT NULL REFERENCES mobile_applications(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, mobile_application_id)
);

CREATE INDEX channel_mobile_applications_application_id_idx
    ON channel_mobile_applications (mobile_application_id);

CREATE TABLE users (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    user_id TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('active', 'deleted')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE push_installations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    user_id TEXT NOT NULL,
    mobile_application_id UUID NOT NULL REFERENCES mobile_applications(id),
    installation_id TEXT NOT NULL,
    token TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, mobile_application_id, installation_id)
);

CREATE INDEX push_installations_active_lookup_idx
    ON push_installations (tenant_id, mobile_application_id, user_id)
    WHERE status = 'active';

CREATE TABLE campaigns (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    channel_id UUID NOT NULL REFERENCES channels(id),
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    image_url TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority TEXT NOT NULL CHECK (priority IN ('critical', 'high', 'normal')),
    status TEXT NOT NULL CHECK (status IN ('draft', 'scheduled', 'starting', 'started', 'completed', 'failed')),
    scheduled_at TIMESTAMPTZ,
    run_id UUID,
    run_attempted_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    failure_reason TEXT NOT NULL DEFAULT '',
    source_batches_total BIGINT NOT NULL DEFAULT 0 CHECK (source_batches_total >= 0),
    source_batches_fanned_out BIGINT NOT NULL DEFAULT 0 CHECK (source_batches_fanned_out >= 0),
    delivery_total BIGINT NOT NULL DEFAULT 0 CHECK (delivery_total >= 0),
    delivery_processed BIGINT NOT NULL DEFAULT 0 CHECK (delivery_processed >= 0),
    delivery_accepted_count BIGINT NOT NULL DEFAULT 0 CHECK (delivery_accepted_count >= 0),
    delivery_failed_count BIGINT NOT NULL DEFAULT 0 CHECK (delivery_failed_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX campaigns_start_candidates_idx
    ON campaigns (status, scheduled_at, run_attempted_at)
    WHERE status IN ('scheduled', 'starting');
CREATE INDEX campaigns_tenant_id_idx ON campaigns (tenant_id);

CREATE TABLE campaign_recipient_batches (
    id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    user_ids JSONB NOT NULL,
    recipient_count INTEGER NOT NULL CHECK (recipient_count > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX campaign_recipient_batches_campaign_id_idx
    ON campaign_recipient_batches (campaign_id);
