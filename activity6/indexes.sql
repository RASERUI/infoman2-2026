-- Author: IGNACIO RUSSELL ROY

-- Scenario 1: Slow Author Profile Page
-- Optimize author profile query (filter + order by date)
CREATE INDEX idx_posts_author_date
ON posts(author_id, date DESC);

-- Scenario 2: The Unsearchable Blog
-- Optimize searches by post title
CREATE INDEX idx_posts_title_prefix
ON posts(title);

-- CREATE INDEX idx_posts_title_gin
-- Scenario 3: Monthly Performance Report
-- Optimize query to fetch all posts in a date range
CREATE INDEX idx_posts_date
ON posts(date);
