CREATE SCHEMA IF NOT EXISTS dds;

-- Таблица друзей
CREATE TABLE IF NOT EXISTS dds.friends (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    friend_id UUID NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    is_best_friend BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id),
    FOREIGN KEY (friend_id) REFERENCES dds.users(id)
);

-- Таблица подписчиков
CREATE TABLE IF NOT EXISTS dds.followers (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    follower_id UUID NOT NULL,
    followed_at VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id),
    FOREIGN KEY (follower_id) REFERENCES dds.users(id)
);

-- Таблица подписок
CREATE TABLE IF NOT EXISTS dds.subscriptions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    subscribed_to UUID NOT NULL,
    subscribed_at VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id),
    FOREIGN KEY (subscribed_to) REFERENCES dds.users(id)
);

-- Таблица блокировок
CREATE TABLE IF NOT EXISTS dds.blocks (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    blocked_id UUID NOT NULL,
    blocked_at VARCHAR(50) NOT NULL,
    reason VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id),
    FOREIGN KEY (blocked_id) REFERENCES dds.users(id)
);

-- Таблица отключенных уведомлений (mutes)
CREATE TABLE IF NOT EXISTS dds.mutes (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    muted_id UUID NOT NULL,
    muted_at VARCHAR(50) NOT NULL,
    duration_days INTEGER,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id),
    FOREIGN KEY (muted_id) REFERENCES dds.users(id)
);

-- Таблица близких друзей
CREATE TABLE IF NOT EXISTS dds.close_friends (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    close_friend_id UUID NOT NULL,
    added_at VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id),
    FOREIGN KEY (close_friend_id) REFERENCES dds.users(id)
);
