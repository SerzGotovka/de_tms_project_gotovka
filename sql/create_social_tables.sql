-- Таблица друзей
CREATE TABLE IF NOT EXISTS raw.friends (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    friend_id UUID NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    is_best_friend BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица подписчиков
CREATE TABLE IF NOT EXISTS raw.followers (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    follower_id UUID NOT NULL,
    followed_at VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица подписок
CREATE TABLE IF NOT EXISTS raw.subscriptions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    subscribed_to UUID NOT NULL,
    subscribed_at VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица блокировок
CREATE TABLE IF NOT EXISTS raw.blocks (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    blocked_id UUID NOT NULL,
    blocked_at VARCHAR(50) NOT NULL,
    reason VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица отключенных уведомлений (mutes)
CREATE TABLE IF NOT EXISTS raw.mutes (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    muted_id UUID NOT NULL,
    muted_at VARCHAR(50) NOT NULL,
    duration_days INTEGER,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица близких друзей
CREATE TABLE IF NOT EXISTS raw.close_friends (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    close_friend_id UUID NOT NULL,
    added_at VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
