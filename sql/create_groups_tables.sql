-- Таблица сообществ
CREATE TABLE IF NOT EXISTS raw.communities (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at VARCHAR(50) NOT NULL,
    member_count INTEGER NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица групп
CREATE TABLE IF NOT EXISTS raw.groups (
    id UUID PRIMARY KEY,
    owner_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    privacy VARCHAR(20) NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица участников групп
CREATE TABLE IF NOT EXISTS raw.group_members (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    joined_at VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица тем сообществ
CREATE TABLE IF NOT EXISTS raw.community_topics (
    id UUID PRIMARY KEY,
    community_id UUID NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица закрепленных постов
CREATE TABLE IF NOT EXISTS raw.pinned_posts (
    id UUID PRIMARY KEY,
    community_id UUID,
    group_id UUID,
    author_id UUID NOT NULL,
    content TEXT NOT NULL,
    pinned_at VARCHAR(50) NOT NULL,
    expires_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
