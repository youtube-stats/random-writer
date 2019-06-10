import atexit
import time
import json
from message import message_pb2
import psycopg2
import requests
from typing import List
from typing import Tuple
from typing import Dict


api_key_server: str = 'http://localhost:8080/get'
write_server: str = 'http://localhost:8081/post'
init_query_sql: str = 'SELECT id, serial FROM youtube.stats.channels ORDER BY RANDOM() LIMIT 50'
user: str = 'admin'
password: str = ''
pg_host: str = 'localhost'
pg_port: str = '5432'
database: str = 'youtube'
chunk_size: int = 50
google_api: str = 'https://www.googleapis.com/youtube/v3/channels?part=statistics&key=%s&id=%s'


def divide_chunks(l: List[Tuple[int, str]], n=chunk_size):
    while True:
        for i in range(0, len(l), n):
            yield l[i:i + n]


def connect() -> psycopg2:
    print('Connecting to db')
    connection: psycopg2 = psycopg2.connect(
        user=user,
        password=password,
        host=pg_host,
        port=pg_port,
        database=database)

    def exit_func() -> None:
        print('Closing connection')
        conn.close()

    atexit.register(exit_func)
    return connection


conn: psycopg2 = connect()


def get_channels() -> List[Tuple[int, str]]:
    cursor = conn.cursor()
    cursor.execute(init_query_sql)

    records = cursor.fetchall()

    return records


store: List[Tuple[int, str]] = get_channels()


def get_api_key() -> str:
    return requests.get(api_key_server).text


def get_metrics(channels: List[str]) -> str:
    key: str = get_api_key()
    ids: str = ','.join(channels)
    url: str = google_api % (key, ids)

    return requests.get(url).text


def metrics_to_protobuf(json_obj: json, idxs: List[int]) -> message_pb2.SubMessage:
    msg: message_pb2.SubMessage = message_pb2.SubMessage()
    msg.timestamp = int(time.time())

    items = json_obj['items']
    for i in range(len(items)):
        item = items[i]
        msg.ids.append(idxs[i])
        sub: int = int(item['statistics']['subscriberCount'])
        msg.subs.append(sub)

    return msg


def serial_to_id(json_obj: json, id_serial: Dict[str, int]) -> List[int]:
    idxs: List[int] = []
    length: int = len(json_obj['items'])

    items = json_obj['items']
    for i in range(length):
        item = items[i]
        idx: int = id_serial[item['id']]
        idxs.append(idx)

    return idxs


def payload_process(chunk: List[Tuple[int, str]]) -> None:
    ids: List[str] = [s for (idx, s) in chunk]

    print('Gathering metrics for', chunk)

    id_serial: Dict[str, int] = {}
    for (i, s) in chunk:
        id_serial[s] = i

    metrics: str = get_metrics(ids)
    json_obj: json = json.loads(metrics)
    idxs: List[int] = serial_to_id(json_obj, id_serial)
    print('Got', len(idxs), 'results from google api')

    proto: message_pb2.SubMessage = metrics_to_protobuf(json_obj, idxs)
    print(str(proto).replace('\n', ', '))

    proto_msg: str = proto.SerializeToString()

    ack: str = requests.post(write_server, data=proto_msg).text
    ack_msg: message_pb2.Ack = message_pb2.Ack()
    ack_msg.ParseFromString(ack.encode())

    if ack_msg.ok:
        print('Message sent succesfully')
    else:
        print('Message failed to be sent')


def main() -> None:
    while True:
        chunk: List[Tuple[int, str]] = get_channels()
        payload_process(chunk)


if __name__ == '__main__':
    main()
