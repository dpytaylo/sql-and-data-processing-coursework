CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS oltp
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname 'oltp');

CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
SERVER oltp
OPTIONS (user 'postgres', password 'password');

-- Step 1: Create a temporary schema and import the foreign schema into it
CREATE SCHEMA IF NOT EXISTS temp_import;

IMPORT FOREIGN SCHEMA public
FROM SERVER oltp
INTO temp_import;

-- Step 2: Rename the imported tables with a prefix "remote"
DO $$
DECLARE
    r RECORD;
    old_table_name TEXT;
    new_table_name TEXT;
BEGIN
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'temp_import'
    LOOP
        old_table_name := quote_ident(r.table_name);
        new_table_name := quote_ident('remote_' || r.table_name);
        EXECUTE 'ALTER TABLE temp_import.' || old_table_name || ' RENAME TO ' || new_table_name || ';';
    END LOOP;
END $$;

-- Step 3: Move the renamed tables to the public schema and drop the temporary schema
DO $$
DECLARE
    r RECORD;
    qualified_table_name  TEXT;
BEGIN
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'temp_import'
    LOOP
        qualified_table_name  := quote_ident(r.table_name);
        EXECUTE 'ALTER TABLE temp_import.' || qualified_table_name  || ' SET SCHEMA public;';
    END LOOP;
END $$;

DROP SCHEMA temp_import;

CREATE TABLE IF NOT EXISTS dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    username VARCHAR(50),
    email VARCHAR(100),
    date_joined TIMESTAMP,
    profile_picture VARCHAR(255),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_video (
    video_sk SERIAL PRIMARY KEY,
    video_id INT NOT NULL,
    user_id INT,
    title VARCHAR(255),
    description TEXT,
    upload_date TIMESTAMP,
    video_url VARCHAR(255),
    thumbnail_url VARCHAR(255),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_sk SERIAL PRIMARY KEY,
    category_id INT NOT NULL,
    name VARCHAR(100),
    description TEXT,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_video_category (
    video_category_sk SERIAL PRIMARY KEY,
    video_id INT NOT NULL,
    category_id INT NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_comment (
    comment_sk SERIAL PRIMARY KEY,
    comment_id INT NOT NULL,
    video_id INT,
    user_id INT,
    content TEXT NOT NULL,
    comment_date TIMESTAMP NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_like (
    like_sk SERIAL PRIMARY KEY,
    video_id INT,
    user_id INT,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_playlist (
    playlist_sk SERIAL PRIMARY KEY,
    playlist_id INT NOT NULL,
    user_id INT,
    name VARCHAR(100),
    description TEXT,
    creation_date TIMESTAMP,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_playlist_video (
    playlist_video_sk SERIAL PRIMARY KEY,
    playlist_id INT NOT NULL,
    video_id INT NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_subscription (
    user_subscription_sk SERIAL PRIMARY KEY,
    subscriber_id INT,
    subscribed_to_id INT,
    subscription_date TIMESTAMP,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_video_category (
    video_category_sk SERIAL PRIMARY KEY,
    video_sk INT,
    category_sk INT,
    FOREIGN KEY (video_sk) REFERENCES dim_video(video_sk),
    FOREIGN KEY (category_sk) REFERENCES dim_category(category_sk)
);

CREATE TABLE IF NOT EXISTS fact_video_user (
    video_user_sk SERIAL PRIMARY KEY,
    video_sk INT,
    user_sk INT,
    FOREIGN KEY (video_sk) REFERENCES dim_video(video_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk)
);

CREATE TABLE IF NOT EXISTS fact_video_like (
    video_like_sk SERIAL PRIMARY KEY,
    video_sk INT,
    likes_count INT,
    FOREIGN KEY (video_sk) REFERENCES dim_video(video_sk)
);
