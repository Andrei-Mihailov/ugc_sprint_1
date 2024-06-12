import vertica_python

connection_info = {
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'dbadmin',
    'password': '',
    'database': 'docker',
    'autocommit': True,
}


def create_table_and_insert_data(connection_info):
    with vertica_python.connect(**connection_info) as connection:
        cursor = connection.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS views (
            id IDENTITY,
            user_id INTEGER NOT NULL,
            movie_id VARCHAR(256) NOT NULL,
            viewed_frame INTEGER NOT NULL
        );
        """)

        cursor.execute(
            """
            INSERT INTO views (user_id, movie_id, viewed_frame) VALUES (
            500271,
            'tt0120338',
            1611902873
            );
            """
        )

        cursor.execute(
            """
            SELECT * FROM views;
            """
        )
        for row in cursor.iterate():
            print(row)


if __name__ == "__main__":
    create_table_and_insert_data(connection_info)
