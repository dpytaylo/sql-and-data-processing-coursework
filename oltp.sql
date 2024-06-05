CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password VARCHAR(100) NOT NULL,
    date_joined TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    profile_picture VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS videos (
    video_id INT PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    upload_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    video_url VARCHAR(255) NOT NULL,
    thumbnail_url VARCHAR(255),
    views_count INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS categories (
    category_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS video_categories (
    video_id INT REFERENCES videos(video_id),
    category_id INT REFERENCES categories(category_id),
    PRIMARY KEY (video_id, category_id)
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id INT PRIMARY KEY,
    video_id INT REFERENCES videos(video_id),
    user_id INT REFERENCES users(user_id),
    content TEXT NOT NULL,
    comment_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS likes (
    video_id INT REFERENCES videos(video_id),
    user_id INT REFERENCES users(user_id),
    PRIMARY KEY (video_id, user_id)
);

CREATE TABLE IF NOT EXISTS playlists (
    playlist_id INT PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    name VARCHAR(100) NOT NULL,
    description TEXT,
    creation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS playlist_videos (
    playlist_id INT REFERENCES playlists(playlist_id),
    video_id INT REFERENCES videos(video_id),
    PRIMARY KEY (playlist_id, video_id)
);

CREATE TABLE IF NOT EXISTS subscriptions (
    subscriber_id INT REFERENCES users(user_id),
    subscribed_to_id INT REFERENCES users(user_id),
    subscription_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (subscriber_id, subscribed_to_id)
);
