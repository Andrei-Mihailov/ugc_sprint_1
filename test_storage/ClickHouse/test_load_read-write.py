import time
import datetime
import random
import multiprocessing
from clickhouse_driver import Client
from tqdm import tqdm

client = Client('localhost')


def insert_data(total_rows):
    insert_query = 'INSERT INTO user_progress (user_id, movie_id, progress, timestamp) VALUES'
    data = [(random.randint(1, 100), random.randint(1, 100), random.random(), datetime.datetime.now()) for _ in
            range(total_rows)]
    client.execute(insert_query, data)


def background_load(total_rows, interval, event):
    while not event.is_set():
        insert_data(total_rows)
        time.sleep(interval)


def read_data():
    select_query = 'SELECT user_id, movie_id, progress FROM user_progress WHERE user_id = 1'
    return client.execute(select_query)


if __name__ == '__main__':
    total_rows = 1000
    interval = 1
    write_iterations = 10
    write_event = multiprocessing.Event()
    background_process = multiprocessing.Process(target=background_load, args=(total_rows, interval, write_event))
    background_process.start()

    start_time = time.time()
    for _ in tqdm(range(write_iterations), desc='Write Iterations', unit='iteration'):
        insert_data(total_rows)
        time.sleep(interval)
    end_time = time.time()

    write_event.set()
    background_process.join()

    write_time = end_time - start_time - interval * (write_iterations - 1)
    write_velocity = (total_rows * write_iterations) / write_time
    print(f'Write Velocity: {write_velocity:.2f} rows/s')

    start_time = time.time()
    read_iterations = 10
    for _ in tqdm(range(read_iterations), desc='Read Iterations', unit='iteration'):
        result = read_data()
        time.sleep(interval)
    end_time = time.time()

    read_time = end_time - start_time - interval * (read_iterations - 1)

