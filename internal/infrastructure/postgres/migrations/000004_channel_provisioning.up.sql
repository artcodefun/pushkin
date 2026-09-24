ALTER TABLE channels
    DROP CONSTRAINT channels_status_check,
    ADD CONSTRAINT channels_status_check
        CHECK (status IN ('provisioning', 'active', 'disabled'));
