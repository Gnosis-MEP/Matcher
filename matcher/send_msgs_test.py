#!/usr/bin/env python
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from matcher.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    LISTEN_EVENT_TYPE_QUERY_REMOVED
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def send_data_msg(service_stream):
    data_msg = {
        'event': json.dumps(
            {
                'id': str(uuid.uuid4()),
                'some': 'data'
            }
        )
    }
    print(f'Sending msg {data_msg}')
    service_stream.write_events(data_msg)


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service_stream = stream_factory.create(SERVICE_STREAM_KEY, stype='streamOnly')
    queryadd_cmd = stream_factory.create(LISTEN_EVENT_TYPE_QUERY_CREATED, stype='streamOnly')
    import ipdb; ipdb.set_trace()

    pubJoin_cmd.write_events(
        new_msg(
            {
                'publisher_id': 'pid',
                'source': 'psource',
                'meta': {
                    'resolution': '300x300',
                    'fps': '30',
                }
            }
        )
    )
    send_data_msg(service_stream)


if __name__ == '__main__':
    main()
