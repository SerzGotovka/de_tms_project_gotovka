CREATE SCHEMA IF NOT EXISTS dds;

-- Таблица постов
CREATE TABLE IF NOT EXISTS dds.posts (
    id INTEGER PRIMARY KEY,
    author_id UUID NOT NULL,
    caption TEXT,
    image_url TEXT,
    location VARCHAR(255),
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (author_id) REFERENCES dds.users(id)
);


-- Таблица историй
CREATE TABLE IF NOT EXISTS dds.stories (
    id INTEGER PRIMARY KEY,
    user_id UUID NOT NULL,
    image_url TEXT NOT NULL,
    caption TEXT,
    expires_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

-- Таблица Reels
CREATE TABLE IF NOT EXISTS dds.reels (
    id INTEGER PRIMARY KEY,
    user_id UUID NOT NULL,
    video_url TEXT NOT NULL,
    caption TEXT,
    music VARCHAR(255),
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);


-- Таблица комментариев
CREATE TABLE IF NOT EXISTS dds.comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    text TEXT NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES dds.posts(id),
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);


-- Таблица ответов на комментарии
CREATE TABLE IF NOT EXISTS dds.replies (
    id INTEGER PRIMARY KEY,
    comment_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    text TEXT NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comment_id) REFERENCES dds.comments(id),
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);


-- Таблица лайков
CREATE TABLE IF NOT EXISTS dds.likes (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES dds.posts(id),
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);


-- Таблица реакций
CREATE TABLE IF NOT EXISTS dds.reactions (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    type VARCHAR(20) NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES dds.posts(id),
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

-- Таблица репостов
CREATE TABLE IF NOT EXISTS dds.shares (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES dds.posts(id),
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

