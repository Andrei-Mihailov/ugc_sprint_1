import time
import datetime
import random
from clickhouse_driver import Client
from tqdm import tqdm

client = Client('localhost')

def insert_data(total_rows):
    insert_query = 'INSERT INTO user_progress (user_id, movie_id, progress, timestamp) VALUES'
    data = [(random.randint(1, 100), random.randint(1, 100), random.random(), datetime.datetime.now()) for _ in tqdm(range(total_rows), desc='Inserting', unit='row')]
    client.execute(insert_query, data)

def read_data():
    select_query = 'SELECT user_id, movie_id, progress FROM user_progress WHERE user_id = 1'
    return client.execute(select_query)

start_time = time.time()
result = read_data()
end_time = time.time()

read_velocity = len(result) / (end_time - start_time)
print(f'Read Velocity: {read_velocity:.2f} rows/s')
print(f'Total time: {(end_time - start_time):.2f} s')

client.disconnect()
