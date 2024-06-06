CREATE OR REPLACE PROCEDURE update_fact_video_category()
AS $$
DECLARE
    v_record RECORD;
    new_video_sk INT;
    new_category_sk INT;
BEGIN
    -- Remove old rows
    DELETE FROM fact_video_category
    WHERE video_sk IN (
        SELECT video_sk FROM dim_video WHERE current_flag = FALSE
    )
    OR category_sk IN (
        SELECT category_sk FROM dim_category WHERE current_flag = FALSE
    );

     FOR v_record IN SELECT * FROM dim_video_category LOOP
        SELECT video_sk INTO new_video_sk
        FROM dim_video
        WHERE video_id = v_record.video_id AND current_flag = TRUE;

        SELECT category_sk INTO new_category_sk
        FROM dim_category
        WHERE category_id = v_record.category_id AND current_flag = TRUE;

        IF new_video_sk IS NOT NULL AND new_category_sk IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1
                FROM fact_video_category
                WHERE video_sk = new_video_sk
                  AND category_sk = new_category_sk
            ) THEN
                INSERT INTO fact_video_category (video_sk, category_sk)
                VALUES (new_video_sk, new_category_sk);
            END IF;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE update_fact_video_like()
AS $$
DECLARE
    v_record RECORD;
    new_video_sk INTEGER;
    new_likes_count INTEGER;
BEGIN
    -- Remove old rows
    DELETE FROM fact_video_like
    USING dim_video
    WHERE fact_video_like.video_sk = dim_video.video_sk AND dim_video.current_flag = FALSE;

    FOR v_record IN SELECT * FROM dim_like LOOP
        SELECT video_sk INTO new_video_sk
        FROM dim_video
        WHERE video_id = v_record.video_id AND current_flag = TRUE;

        -- Assuming likes_count is incremented for each like record
        SELECT COUNT(*) INTO new_likes_count
        FROM dim_like
        WHERE video_id = v_record.video_id AND current_flag = TRUE;

        IF new_video_sk IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1
                FROM fact_video_like
                WHERE video_sk = new_video_sk
            ) THEN
                INSERT INTO fact_video_like (video_sk, likes_count)
                VALUES (new_video_sk, new_likes_count);
            ELSE
                UPDATE fact_video_like
                SET likes_count = new_likes_count
                WHERE video_sk = new_video_sk;
            END IF;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE update_fact_video_user()
AS $$
DECLARE
    v_record RECORD;
    new_video_sk INTEGER;
    new_user_sk INTEGER;
BEGIN
    -- Remove old rows
    DELETE FROM fact_video_user
    USING dim_video, dim_user
    WHERE fact_video_user.video_sk = dim_video.video_sk AND dim_video.current_flag = FALSE
       OR fact_video_user.user_sk = dim_user.user_sk AND dim_user.current_flag = FALSE;

    FOR v_record IN SELECT * FROM dim_video LOOP
        SELECT video_sk INTO new_video_sk
        FROM dim_video
        WHERE video_id = v_record.video_id AND current_flag = TRUE;

        SELECT user_sk INTO new_user_sk
        FROM dim_user
        WHERE user_id = v_record.user_id AND current_flag = TRUE;

        IF new_video_sk IS NOT NULL AND new_user_sk IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1
                FROM fact_video_user
                WHERE video_sk = new_video_sk
                  AND user_sk = new_user_sk
            ) THEN
                INSERT INTO fact_video_user (video_sk, user_sk)
                VALUES (new_video_sk, new_user_sk);
            END IF;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE update_fact_tables()
AS $$
BEGIN
    CALL update_fact_video_category();
    CALL update_fact_video_like();
    CALL update_fact_video_user();
END
$$ LANGUAGE plpgsql;

CALL update_fact_tables();
