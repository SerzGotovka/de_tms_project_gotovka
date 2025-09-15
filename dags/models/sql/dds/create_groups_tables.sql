CREATE SCHEMA IF NOT EXISTS dds;


-- Таблица сообществ
CREATE TABLE IF NOT EXISTS dds.communities (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at VARCHAR(50) NOT NULL,
    member_count INTEGER NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица групп
CREATE TABLE IF NOT EXISTS dds.groups (
    id UUID PRIMARY KEY,
    owner_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    privacy VARCHAR(20) NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (owner_id) REFERENCES dds.users(id)
);

-- Таблица участников групп
CREATE TABLE IF NOT EXISTS dds.group_members (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    user_id UUID NOT NULL,
    joined_at VARCHAR(50) NOT NULL,
    role VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES dds.groups(id),
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

-- Таблица тем сообществ
CREATE TABLE IF NOT EXISTS dds.community_topics (
    id UUID PRIMARY KEY,
    community_id UUID NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    created_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (community_id) REFERENCES dds.communities(id)
);

-- Таблица закрепленных постов
CREATE TABLE IF NOT EXISTS dds.pinned_posts (
    id UUID PRIMARY KEY,
    community_id UUID,
    group_id UUID,
    author_id UUID NOT NULL,
    content TEXT NOT NULL,
    pinned_at VARCHAR(50) NOT NULL,
    expires_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (community_id) REFERENCES dds.communities(id),
    FOREIGN KEY (group_id) REFERENCES dds.groups(id),
    FOREIGN KEY (author_id) REFERENCES dds.users(id)
);
