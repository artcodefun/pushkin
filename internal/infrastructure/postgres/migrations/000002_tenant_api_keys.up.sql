CREATE TABLE tenant_api_keys (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    secret_hash BYTEA NOT NULL CHECK (octet_length(secret_hash) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    UNIQUE (tenant_id, name)
);

CREATE INDEX tenant_api_keys_active_lookup_idx
    ON tenant_api_keys (id)
    WHERE revoked_at IS NULL;
