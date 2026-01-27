-- PostgreSQL Blog Schema

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    body TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    comment TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Initial Data
INSERT INTO users (username) VALUES
('alice'),
('bob'),
('charlie'),
('david'),
('emma'),
('frank'),
('grace');

INSERT INTO posts (user_id, title, body) VALUES
(1, 'First Post!', 'This is the body of the first post.'),
(2, 'Bob''s Thoughts', 'A penny for my thoughts.'),
(3, 'Hello World', 'My first blog post!'),
(4, 'Database Fun', 'Learning PostgreSQL is fun.'),
(5, 'SQL Tips', 'Always test your queries.'),
(6, 'Tech Talk', 'Let’s talk about tech.'),
(7, 'Final Post', 'Wrapping things up.');

INSERT INTO comments (post_id, user_id, comment) VALUES
(1, 2, 'Great first post, Alice!'),
(2, 1, 'Interesting thoughts, Bob.'),
(3, 4, 'Nice introduction!'),
(4, 5, 'Very helpful, thanks!'),
(5, 6, 'Good advice.'),
(6, 7, 'Looking forward to more posts.'),
(7, 3, 'Well written!');
