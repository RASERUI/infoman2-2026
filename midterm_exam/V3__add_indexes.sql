-- Indexes for reports table
CREATE INDEX IF NOT EXISTS idx_reports_status    ON reports (status);
CREATE INDEX IF NOT EXISTS idx_reports_timestamp ON reports (timestamp);
CREATE INDEX IF NOT EXISTS idx_reports_reporter  ON reports (reporter);

-- Index for users table
CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);
