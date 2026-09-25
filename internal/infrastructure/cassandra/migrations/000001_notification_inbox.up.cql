CREATE TABLE IF NOT EXISTS notifications_by_user (
  tenant_id text,
  user_id text,
  created_at timestamp,
  notification_id text,
  campaign_id text,
  title text,
  body text,
  image_url text,
  data text,
  PRIMARY KEY ((tenant_id, user_id), created_at, notification_id)
) WITH CLUSTERING ORDER BY (created_at DESC, notification_id DESC)
  AND default_time_to_live = 7776000
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

CREATE TABLE IF NOT EXISTS notification_read_state_by_user (
  tenant_id text,
  user_id text,
  notification_id text,
  read_at timestamp,
  PRIMARY KEY ((tenant_id, user_id), notification_id)
) WITH default_time_to_live = 7776000
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};
