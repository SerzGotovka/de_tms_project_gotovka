-- Таблица постов
CREATE TABLE IF NOT EXISTS raw.posts (
    id INTEGER PRIMARY KEY,
    author_id UUID NOT NULL,
    caption TEXT,
    image_url TEXT,
    location VARCHAR(255),
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица историй
CREATE TABLE IF NOT EXISTS raw.stories (
    id INTEGER PRIMARY KEY,
    user_id UUID NOT NULL,
    image_url TEXT NOT NULL,
    caption TEXT,
    expires_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица Reels
CREATE TABLE IF NOT EXISTS raw.reels (
    id INTEGER PRIMARY KEY,
    user_id UUID NOT NULL,
    video_url TEXT NOT NULL,
    caption TEXT,
    music VARCHAR(255),
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица комментариев
CREATE TABLE IF NOT EXISTS raw.comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    text TEXT NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица ответов на комментарии
CREATE TABLE IF NOT EXISTS raw.replies (
    id INTEGER PRIMARY KEY,
    comment_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    text TEXT NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица лайков
CREATE TABLE IF NOT EXISTS raw.likes (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица реакций
CREATE TABLE IF NOT EXISTS raw.reactions (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    type VARCHAR(20) NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица репостов
CREATE TABLE IF NOT EXISTS raw.shares (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id UUID NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

