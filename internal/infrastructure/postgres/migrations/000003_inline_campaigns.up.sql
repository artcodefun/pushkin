ALTER TABLE campaigns
    ADD COLUMN recipient_mode TEXT NOT NULL DEFAULT 'batched'
        CHECK (recipient_mode IN ('batched', 'inline')),
    ADD COLUMN inline_recipients JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE campaigns
    ADD CONSTRAINT campaigns_inline_recipients_check CHECK (
        (recipient_mode = 'batched' AND inline_recipients = '[]'::jsonb)
        OR (
            recipient_mode = 'inline'
            AND jsonb_typeof(inline_recipients) = 'array'
            AND jsonb_array_length(inline_recipients) BETWEEN 1 AND 100
        )
    );
