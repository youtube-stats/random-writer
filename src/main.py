import csv
import json
from message import message_pb2
import random
import requests
import time
from typing import List
from typing import Tuple
from typing import Dict


api_key_server: str = 'http://localhost:8080/get'
write_server: str = 'http://localhost:8081/post'
chunk_size: int = 50
google_api: str = 'https://www.googleapis.com/youtube/v3/channels?part=statistics&key=%s&id=%s'


def get_channels() -> List[Tuple[int, str]]:
    csv_file = open('./channels.csv', 'r')
    csv_reader = csv.reader(csv_file, delimiter=',')

    records: List[Tuple[int, str]] = []
    for (idx, serial) in csv_reader:
        records.append((int(idx), serial))

    print('Retrieved', len(records), 'records')
    return records


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
    chans: List[Tuple[int, str]] = get_channels()

    while True:
        chunk: List[Tuple[int, str]] = random.choices(chans, k=50)
        payload_process(chunk)


if __name__ == '__main__':
    main()
