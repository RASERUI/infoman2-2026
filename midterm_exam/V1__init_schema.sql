CREATE TABLE IF NOT EXISTS users (
    id       BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role     VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS reports (
    id          BIGSERIAL PRIMARY KEY,
    reporter    VARCHAR(255),
    contact     VARCHAR(255),
    location    VARCHAR(255),
    description TEXT,
    status      VARCHAR(50),
    timestamp   VARCHAR(255)
);
