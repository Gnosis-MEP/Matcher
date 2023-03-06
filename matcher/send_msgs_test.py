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




EVENT_DATA_MSG = """
{
    "id": "WindowManager:bcde1b50-927e-41ab-ad50-e88975d4cc24",
    "vekg_stream": [
        {
            "id": "publisher1-15d9fa62-5840-41ad-b795-dbaa2f0e9bc6",
            "publisher_id": "publisher1",
            "source": "rtmp://172.17.0.1/vod2/coco2017-val-300x300-60fps.flv",
            "image_url": "d99ea781-bc9a-44ec-bd55-702d4491e2b8",
            "vekg": {
                "nodes": [
                    [
                        "443c6cf5-6d3d-481d-9237-edd03d9d28ff",
                        {
                            "id": "443c6cf5-6d3d-481d-9237-edd03d9d28ff",
                            "label": "person",
                            "confidence": 0.790602445602417,
                            "bounding_box": [
                                130,
                                27,
                                227,
                                272
                            ]
                        }
                    ]
                ]
            },
            "query_ids": [
                "d16591a615e99c5c76ac7e8a38be1f32"
            ],
            "width": 300,
            "height": 300,
            "color_channels": "BGR",
            "frame_index": 5,
            "tracer": {
                "headers": {
                    "uber-trace-id": "b09c055f9e01b2d4:d8e7ad934f891940:1850301624d29b62:1"
                }
            },
            "buffer_stream_key": "5eae231b600f83743165d3ea3f3c53a4",
            "data_flow": [
                [
                    "object-detection-ssd-gpu-data"
                ],
                [
                    "wm-data"
                ]
            ],
            "data_path": [
                "object-detection-ssd-gpu-data"
            ]
        },
        {
            "id": "publisher1-6d48eff5-cd98-4af6-beaf-c723f061ab95",
            "publisher_id": "publisher1",
            "source": "rtmp://172.17.0.1/vod2/coco2017-val-300x300-60fps.flv",
            "image_url": "4e2759ac-c1e3-4c05-b7d2-f01065af72eb",
            "vekg": {
                "nodes": [
                    [
                        "0c5ff0b1-04b9-48f8-a721-56c8a6f79750",
                        {
                            "id": "0c5ff0b1-04b9-48f8-a721-56c8a6f79750",
                            "label": "person",
                            "confidence": 0.7414940595626831,
                            "bounding_box": [
                                95,
                                78,
                                130,
                                247
                            ]
                        }
                    ],
                    [
                        "6146d15b-8de1-4008-b7f5-54d7429c35dd",
                        {
                            "id": "6146d15b-8de1-4008-b7f5-54d7429c35dd",
                            "label": "person",
                            "confidence": 0.7386733293533325,
                            "bounding_box": [
                                21,
                                116,
                                57,
                                246
                            ]
                        }
                    ],
                    [
                        "a2e43bc7-5355-48a8-bf8c-1ceb5942c1d1",
                        {
                            "id": "a2e43bc7-5355-48a8-bf8c-1ceb5942c1d1",
                            "label": "person",
                            "confidence": 0.6852085590362549,
                            "bounding_box": [
                                188,
                                129,
                                243,
                                288
                            ]
                        }
                    ],
                    [
                        "de94a864-1bbd-47d0-ade0-f77df713b43a",
                        {
                            "id": "de94a864-1bbd-47d0-ade0-f77df713b43a",
                            "label": "person",
                            "confidence": 0.5655462741851807,
                            "bounding_box": [
                                119,
                                63,
                                161,
                                247
                            ]
                        }
                    ]
                ]
            },
            "query_ids": [
                "d16591a615e99c5c76ac7e8a38be1f32"
            ],
            "width": 300,
            "height": 300,
            "color_channels": "BGR",
            "frame_index": 9,
            "tracer": {
                "headers": {
                    "uber-trace-id": "4db15c314d02306b:eebbccc2c06b5f5c:293817dad5db9aec:1"
                }
            },
            "buffer_stream_key": "5eae231b600f83743165d3ea3f3c53a4",
            "data_flow": [
                [
                    "object-detection-ssd-gpu-data"
                ],
                [
                    "wm-data"
                ]
            ],
            "data_path": [
                "object-detection-ssd-gpu-data"
            ]
        }
    ],
    "query_id": "d16591a615e99c5c76ac7e8a38be1f32",
    "tracer": {
        "headers": {
            "uber-trace-id": "b09c055f9e01b2d4:9cbb5b0e21809477:e614907db60f83f3:1"
        }
    }
}"""

EVENT_DATA_JSON = json.loads(EVENT_DATA_MSG)


# REGISTER QUERY countCupeees OUTPUT AnnotatedRetVideoStream CONTENT ObjectDetection MATCH (p:person) FROM publisher1 WITHIN TUMBLING_COUNT_WINDOW(1) WITH_QOS accuracy = 5, latency = 8, energy_consumption = 3 RETURN p, count(p) as PeopleCount

NEW_QUERY_JSON = {
    "subscriber_id": "sub_id",
    "query_id": "d16591a615e99c5c76ac7e8a38be1f32",
    "parsed_query": {
        "name": "aPerson",
        "output": [
            "AnnotatedVideoStream"
        ],
        "from": [
            "publisher1"
        ],
        "content": [
            "ObjectDetection"
        ],
        "match": "MATCH (p:person)-[:same_frame]-(:person)",
        "optional_match": "",
        "where": "",
        "window": {
            "window_type": "TUMBLING_COUNT_WINDOW",
            "args": [
                1
            ]
        },
        "ret": "RETURN count(distinct p) as PersonCount",
        "qos_policies": {
            "accuracy": 5.0,
            "latency": 8.0,
            "energy_consumption": 3.0
        }
    },
    "query_received_event_id": "AccessPoint:f92461c6-e292-403f-8e52-52cb84986622",
    "buffer_stream": {
        "publisher_id": "publisher1",
        "buffer_stream_key": "60f0bf97c4de61f5a06b99303d2c24fa",
        "source": "rtmp://172.17.0.1/hls/mystream",
        "resolution": "1280x720",
        "fps": "10"
    },
    "service_chain": [],
    "id": "ClientManager:560d119d-942e-4749-a9d2-ae0f91646d4b",
    "tracer": {
        "headers": {
            "uber-trace-id": "c812d74b9f79cc45:ca6e1677a87dfc52:1c2b78de1a3a6fe9:1"
        }
    }
}


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def send_data_msg(service_stream):
    data_msg = {'event': EVENT_DATA_JSON}
    print(f'Sending msg {data_msg}')
    service_stream.write_events(data_msg)


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service_stream = stream_factory.create(SERVICE_STREAM_KEY, stype='streamOnly')
    queryadd_cmd = stream_factory.create(LISTEN_EVENT_TYPE_QUERY_CREATED, stype='streamOnly')
    import ipdb; ipdb.set_trace()

    queryadd_cmd.write_events(
        new_msg(NEW_QUERY_JSON)
    )

    service_stream.write_events({'event': json.dumps(EVENT_DATA_JSON)})


if __name__ == '__main__':
    main()


# query_res = query_graph.query('MATCH (p:person)-[r:same_frame]-(:person) RETURN p, count(distinct r)')
# query_res_dict = {}
# for res_row in query_res.result_set:
#     for col_index, (coltype, bcol) in enumerate(query_res.header):
#         col = bcol.decode('utf-8')
#         dict_row = query_res_dict.setdefault(col, [])
#         col_res = res_row[col_index]
#         clean_col_res = None
#         if isinstance(col_res, Node):
#             clean_col_res = col_res.properties
#         elif isinstance(col_res, Edge):
#             clean_col_res = {'src_node':col_res.src_node, 'relation': col_res.relation, 'dest_node': col_res.dest_node}
#         else:
#             clean_col_res = col_res
#         dict_row.append(clean_col_res)