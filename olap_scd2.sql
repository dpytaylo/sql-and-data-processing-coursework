CREATE OR REPLACE FUNCTION get_column_value_from_record(
    p_record RECORD,
    p_column_name TEXT
)
RETURNS TEXT AS $$
DECLARE
    json_record JSONB;
    result TEXT;
BEGIN
    -- Convert the record to JSONB
    json_record := to_jsonb(p_record);

    -- Extract the value of the specified column from the JSONB record
    result := json_record ->> p_column_name;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION are_records_equal(
    p_record1 RECORD,
    p_record2 RECORD,
    p_fields TEXT[]
)
RETURNS BOOLEAN AS $$
DECLARE
    field TEXT;
    sql_query TEXT;
    is_equal BOOLEAN := TRUE;
BEGIN
    FOREACH field IN ARRAY p_fields
    LOOP
        sql_query := format(
            'SELECT %L IS NOT DISTINCT FROM %L',
            get_column_value_from_record(p_record1, field),
            get_column_value_from_record(p_record2, field)
        );

        EXECUTE sql_query INTO is_equal USING p_record1, p_record2;

        IF NOT is_equal THEN
            RETURN FALSE;
        END IF;
    END LOOP;

    RETURN is_equal;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION manage_scd_type2(
    p_dimension_table TEXT,
    p_staging_table TEXT,
    p_key_columns TEXT[],
    p_change_columns TEXT[],
    p_other_columns TEXT[]
)
RETURNS VOID AS $$
DECLARE
    all_columns TEXT[];
    v_rem_record RECORD;
    v_dim_record RECORD;
    v_condition TEXT;
    v_exists BOOLEAN;
    v_current_time TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    all_columns = p_key_columns || p_change_columns || p_other_columns;

    FOR v_rem_record IN EXECUTE 'SELECT * FROM ' || p_staging_table LOOP
        v_condition = array_to_string(
            ARRAY(
                SELECT format('%I.%I = %L', p_dimension_table, col, get_column_value_from_record(v_rem_record, col))
                FROM unnest(p_key_columns) col
            ),
            ' AND '
        );

        EXECUTE 'SELECT EXISTS (SELECT 1 FROM ' || p_dimension_table || ' WHERE current_flag = TRUE AND ' || v_condition || ')' INTO v_exists;

        IF v_exists THEN
            EXECUTE 'SELECT * FROM ' || p_dimension_table || ' WHERE current_flag = TRUE AND ' || v_condition INTO v_dim_record;

            IF NOT are_records_equal(v_rem_record, v_dim_record, p_change_columns) THEN
                EXECUTE format(
                    'UPDATE %I SET end_date = %L, current_flag = FALSE WHERE %s AND current_flag = TRUE',
                    p_dimension_table,
                    v_current_time,
                    v_condition
                );

                EXECUTE format(
                    'INSERT INTO %I (%s, start_date, end_date, current_flag) VALUES (%s, %L, NULL, TRUE)',
                    p_dimension_table,
                    array_to_string(all_columns, ', '),
                    array_to_string(
                        ARRAY(
                            SELECT format('%L', get_column_value_from_record(v_rem_record, col))
                            FROM unnest(all_columns) col
                        ),
                        ', '
                    ),
                    v_current_time
                );
            END IF;
        ELSE
            EXECUTE format(
                'INSERT INTO %I (%s, start_date, end_date, current_flag) VALUES (%s, %L, NULL, TRUE)',
                p_dimension_table,
                array_to_string(all_columns, ', '),
                array_to_string(
                    ARRAY(
                        SELECT format('%L', get_column_value_from_record(v_rem_record, col))
                        FROM unnest(all_columns) col
                    ),
                    ', '
                ),
                v_current_time
            );
        END IF;

    END LOOP;

    -- Checking for removed rows
    FOR v_dim_record IN EXECUTE 'SELECT * FROM ' || p_dimension_table || ' WHERE current_flag = TRUE' LOOP
        v_condition = array_to_string(
            ARRAY(
                SELECT format('%I.%I = %L', p_staging_table, col, get_column_value_from_record(v_dim_record, col))
                FROM unnest(p_key_columns) col
            ),
            ' AND '
        );

        EXECUTE 'SELECT EXISTS (SELECT 1 FROM ' || p_staging_table || ' WHERE ' || v_condition || ')' INTO v_exists;

        IF NOT v_exists THEN
            v_condition = array_to_string(
                ARRAY(
                    SELECT format('%I.%I = %L', p_dimension_table, col, get_column_value_from_record(v_dim_record, col))
                    FROM unnest(p_key_columns) col
                ),
                ' AND '
            );

            EXECUTE format(
                'UPDATE %I SET end_date = %L, current_flag = FALSE WHERE %s AND current_flag = TRUE',
                p_dimension_table,
                v_current_time,
                v_condition
            );
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

SELECT manage_scd_type2(
    'dim_category',
    'remote_categories',
    ARRAY['category_id'],
    ARRAY['name', 'description'],
    ARRAY[]::TEXT[]
);

SELECT manage_scd_type2(
    'dim_comment',
    'remote_comments',
    ARRAY['comment_id'],
    ARRAY['video_id', 'user_id', 'content', 'comment_date'],
    ARRAY[]::TEXT[]
);

SELECT manage_scd_type2(
    'dim_playlist',
    'remote_playlists',
    ARRAY['playlist_id'],
    ARRAY['user_id', 'name', 'description', 'creation_date'],
    ARRAY[]::TEXT[]
);

SELECT manage_scd_type2(
    'dim_subscription',
    'remote_subscriptions',
    ARRAY['subscriber_id', 'subscribed_to_id'],
    ARRAY['subscription_date'],
    ARRAY[]::TEXT[]
);

SELECT manage_scd_type2(
    'dim_user',
    'remote_users',
    ARRAY['user_id'],
    ARRAY['username', 'email', 'date_joined', 'profile_picture'],
    ARRAY[]::TEXT[]
);

SELECT manage_scd_type2(
    'dim_video',
    'remote_videos',
    ARRAY['video_id'],
    ARRAY['user_id', 'title', 'description', 'upload_date', 'video_url', 'thumbnail_url'],
    ARRAY[]::TEXT[]
);

-- Likes should be uploaded after videos (update_fact_video_likes() requires it)
SELECT manage_scd_type2(
    'dim_like',
    'remote_likes',
    ARRAY['video_id', 'user_id'],
    ARRAY[]::TEXT[],
    ARRAY[]::TEXT[]
);
