-- Создание таблиц для пользователей в схеме raw

-- Таблица пользователей
CREATE TABLE IF NOT EXISTS raw.users (
    id UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    last_login VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL,
    is_verified BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица профилей пользователей
CREATE TABLE IF NOT EXISTS raw.user_profiles (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    bio TEXT,
    avatar_url TEXT,
    cover_photo_url TEXT,
    location VARCHAR(255),
    website TEXT,
    phone VARCHAR(50),
    birth_date VARCHAR(20),
    gender VARCHAR(20) NOT NULL,
    badges TEXT[], -- Массив строк для бейджей
    updated_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица настроек пользователей
CREATE TABLE IF NOT EXISTS raw.user_settings (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    language VARCHAR(10) NOT NULL,
    timezone VARCHAR(10) NOT NULL,
    theme VARCHAR(10) NOT NULL,
    notifications_enabled BOOLEAN NOT NULL,
    email_notifications BOOLEAN NOT NULL,
    push_notifications BOOLEAN NOT NULL,
    two_factor_enabled BOOLEAN NOT NULL,
    auto_save_drafts BOOLEAN NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    updated_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица настроек приватности пользователей
CREATE TABLE IF NOT EXISTS raw.user_privacy (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    profile_visibility VARCHAR(20) NOT NULL,
    posts_visibility VARCHAR(20) NOT NULL,
    friends_visibility VARCHAR(20) NOT NULL,
    photos_visibility VARCHAR(20) NOT NULL,
    contact_info_visibility VARCHAR(20) NOT NULL,
    search_visibility BOOLEAN NOT NULL,
    allow_friend_requests BOOLEAN NOT NULL,
    allow_messages BOOLEAN NOT NULL,
    show_online_status BOOLEAN NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    updated_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Таблица статусов пользователей
CREATE TABLE IF NOT EXISTS raw.user_status (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    status_message TEXT,
    last_seen VARCHAR(50) NOT NULL,
    is_online BOOLEAN NOT NULL,
    created_at VARCHAR(50) NOT NULL,
    updated_at VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

