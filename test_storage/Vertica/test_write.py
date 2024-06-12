import datetime
import concurrent.futures
import time
from tqdm import tqdm

import vertica_python

connection_info = {
    'host': '127.0.0.1',
    'port': 5433,
    'user': 'dbadmin',
    'password': '',
    'database': 'docker',
    'autocommit': True,
}

def generate_entries(start_index, end_index):
    entries = []
    for i in range(start_index, end_index):
        progress = i / 1000.0
        timestamp = current_timestamp + datetime.timedelta(seconds=i)
        entry = (user_id, movie_id, progress, timestamp)
        entries.append(entry)
    return entries

def insert_entries(entries):
    with vertica_python.connect(**connection_info) as connection:
        cursor = connection.cursor()
        insert_query = 'INSERT INTO user_progress (user_id, movie_id, progress, timestamp) VALUES (?, ?, ?, ?)'
        cursor.executemany(insert_query, entries)

user_id = 1
movie_id = 123
current_timestamp = datetime.datetime.now()

batch_size = 10000
total_entries = 300000
batches = total_entries // batch_size

if __name__ == '__main__':
    print("Connecting to db")

    print("Creating entries")
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        entries = list(tqdm(executor.map(generate_entries, [i*batch_size for i in range(batches)], [(i+1)*batch_size for i in range(batches)]), total=batches))
    print("Rows/s during creation: {:.2f}".format(total_entries / (time.time() - start_time)))

    print("Inserting entries")
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(insert_entries, entries), total=batches))
    print("Rows/s during insertion: {:.2f}".format(total_entries / (time.time() - start_time)))
