import time
import datetime
import random
import vertica_python
from tqdm import tqdm

connection_info = {
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'dbadmin',
    'password': '',
    'database': 'docker',
    'autocommit': True,
}

def insert_data(total_rows):
    insert_query = 'INSERT INTO user_progress (user_id, movie_id, progress, timestamp) VALUES (%s, %s, %s, %s)'
    with vertica_python.connect(**connection_info) as connection:
        cursor = connection.cursor()
        data = [(random.randint(1, 100), random.randint(1, 100), random.random(), datetime.datetime.now()) for _ in tqdm(range(total_rows), desc='Inserting', unit='row')]
        cursor.executemany(insert_query, data)

def read_data(batch_size):
    select_query = 'SELECT * FROM user_progress LIMIT %s OFFSET %s'
    with vertica_python.connect(**connection_info) as connection:
        cursor = connection.cursor()
        offset = 0
        while True:
            cursor.execute(select_query, (batch_size, offset))
            result = cursor.fetchall()
            if not result:
                break
            yield result
            offset += batch_size

batch_size = 100000
start_time = time.time()
read_velocity = 0
total_rows = 0
with tqdm(desc='Reading', unit='batch') as pbar:
    for result in read_data(batch_size):
        total_rows += len(result)
        read_velocity = total_rows / (time.time() - start_time)
        pbar.set_postfix(velocity=f'{read_velocity:.2f} rows/s')
        pbar.update(1)

print(f'Total rows fetched: {total_rows}')
print(f'Total time: {(time.time() - start_time):.2f} s')

