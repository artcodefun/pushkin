ALTER TABLE campaigns
    DROP CONSTRAINT campaigns_inline_recipients_check,
    DROP COLUMN inline_recipients,
    DROP COLUMN recipient_mode;
