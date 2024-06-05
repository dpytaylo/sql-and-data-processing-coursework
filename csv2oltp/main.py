import logging
import os
import psycopg2
import pandas as pd

from dotenv import load_dotenv

from logger import logger

def clean_data(df):
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace(r'[^\w\s]', '', regex=True).str.strip()

    return df

# Creates tables if not exist
def create_tables(cursor, db_init_path):
    with open(db_init_path, 'r') as f:
        create_table_queries = f.read()
    
    logger.info(f'Initializing database\n{create_table_queries}')
    cursor.execute(create_table_queries)

def load_and_insert_data(cursor, table_name, file_path, unique_columns):
    df = pd.read_csv(file_path)
    df = clean_data(df)
    
    # Check for existing data
    unique_conditions = " AND ".join([f"{col} = %s" for col in unique_columns])
    for _, row in df.iterrows():
        try:
            cursor.execute("SAVEPOINT before_insert")

            select_query = f"SELECT 1 FROM {table_name} WHERE {unique_conditions}"
            cursor.execute(select_query, tuple(row[col] for col in unique_columns))
            if not cursor.fetchone():
                columns = ', '.join(row.index)
                values = ', '.join(['%s'] * len(row))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                cursor.execute(insert_query, tuple(row))

            cursor.execute("RELEASE SAVEPOINT before_insert")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT before_insert")
            logger.error(f"Failed to insert row into {table_name}: {row.to_dict()}")
            logger.error(f"Error: {e}")

def main():
    logger.info("Start to load csv data into database")
    load_dotenv()

    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_init = os.getenv('DB_INIT')
    csv_dir = os.getenv('CSV_DIR')

    conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)
    cursor = conn.cursor()
    
    create_tables(cursor, db_init)
    
    # Table name : PK
    csv_files = {
        'users': ['username', 'email'],
        'videos': ['video_id'],
        'categories': ['category_id'],
        'video_categories': ['video_id', 'category_id'],
        'comments': ['comment_id'],
        'likes': ['video_id', 'user_id'],
        'subscriptions': ['subscriber_id', 'subscribed_to_id'],
        'playlists': ['playlist_id'],
        'playlist_videos': ['playlist_id', 'video_id']
    }
    
    for table, unique_cols in csv_files.items():
        file_path = os.path.join(csv_dir, f"{table}.csv")
        load_and_insert_data(cursor, table, file_path, unique_cols)
    
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Successfully finished loading csv data into database")

if __name__ == '__main__':
    main()
