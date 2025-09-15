CREATE SCHEMA IF NOT EXISTS dds;


-- Таблица фотографий
CREATE TABLE IF NOT EXISTS dds.photos (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    filename VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    description TEXT,
    uploaded_at TIMESTAMP NOT NULL,
    is_private BOOLEAN NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

-- Таблица видео
CREATE TABLE IF NOT EXISTS dds.videos (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    title VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    duration_seconds INTEGER NOT NULL,
    uploaded_at TIMESTAMP NOT NULL,
    visibility VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

-- Таблица альбомов
CREATE TABLE IF NOT EXISTS dds.albums (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL,
    media_ids TEXT[], -- Массив ID медиафайлов
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dds.users(id)
);

