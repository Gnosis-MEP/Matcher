from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedEventDrivenServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from matcher.service import Matcher

from matcher.conf import (
    SERVICE_STREAM_KEY,
    FORWARDER_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    SERVICE_DETAILS,
    PUB_EVENT_LIST,
)


class TestMatcher(MockedEventDrivenServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key_list': SERVICE_CMD_KEY_LIST,
        'pub_event_list': PUB_EVENT_LIST,
        'service_details': SERVICE_DETAILS,
        'graph_db_api': lambda x: (),
        'forwarder_stream_key': FORWARDER_STREAM_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = Matcher

    MOCKED_CG_STREAM_DICT = {

    }
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        'cg-Matcher': MOCKED_CG_STREAM_DICT,
    }

    @patch('matcher.service.Matcher.process_event_type')
    def test_process_cmd_should_call_process_event_type(self, mocked_process_event_type):
        event_type = 'SomeEventType'
        unicode_event_type = event_type.encode('utf-8')
        event_data = {
            'id': 1,
            'action': event_type,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_event_type.__name__ = 'process_event_type'

        self.service.service_cmd.mocked_values_dict = {
            unicode_event_type: [msg_tuple]
        }
        self.service.process_cmd()
        self.assertTrue(mocked_process_event_type.called)
        self.service.process_event_type.assert_called_once_with(event_type=event_type, event_data=event_data, json_msg=msg_tuple[1])

    @patch('matcher.service.Matcher.add_query_matching_action')
    def test_process_event_type_should_call_add_query_matching_with_proper_parameters(self, mocked_add_query_m):
        parsed_query = {
            'from': ['pub1'],
            'content': ['ObjectDetection', 'ColorDetection'],
            'window': {
                'window_type': 'TUMBLING_COUNT_WINDOW',
                'args': [2]
            },
            'match': "MATCH (c1:Car {color:'blue'}), (c2:Car {color:'white'})",
            'optional_match': 'optional_match',
            'where': 'where',
            'ret': 'RETURN *',
            # 'cypher_query': query['cypher_query'],
        }
        event_data = {
            'id': 1,
            'query_id': 'query-id',
            'subscriber_id': 'subscriber_id',
            'parsed_query': parsed_query,
        }
        event_type = 'QueryCreated'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_add_query_m.assert_called_once_with(
            query_id=event_data['query_id'],
            match=parsed_query['match'],
            optional_match=parsed_query['optional_match'],
            where=parsed_query['where'],
            ret=parsed_query['ret']
        )

    def test_add_query_matching_action_should_update_datastructure(self):
        query_id = 'query_id1'
        match = 'MATCH (c1:Car), (c2:Car {color:"white"})'
        where = 'WHERE c1.color = "blue" OR c1.color = "black"'
        optional_match = 'OPTIONAL MATCH (c1:Car), (c2:Car {color:"white"})'
        ret = 'RETURN c1, c2'
        self.service.add_query_matching_action(query_id, match, optional_match, where, ret)
        self.assertIn(query_id, self.service.query_matching.keys())
        self.assertDictEqual(
            {
                'match': match,
                'optional_match': optional_match,
                'where': where,
                'ret': ret,
                'cypher_query': (
                    'MATCH (c1:Car), (c2:Car {color:"white"})'
                    ' OPTIONAL MATCH (c1:Car), (c2:Car {color:"white"})'
                    ' WHERE c1.color = "blue" OR c1.color = "black"'
                    ' RETURN c1, c2'
                )
            },
            self.service.query_matching[query_id]
        )

    @patch('matcher.service.Matcher.match_query')
    def test_process_data_event_should_call_match_query_with_proper_parameters(self, mocked_match_query):
        event_data = {
            'id': 1,
            'vekg_stream': [1, 2, 3],
            'query_id': 'query_id'
        }
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_data_event(event_data, json_msg)
        mocked_match_query.assert_called_once_with(
            event_data['query_id'],
            event_data['vekg_stream']
        )

    def test_re_create_cypher_query(self):
        cypher = self.service.re_create_cypher_query('match', 'optional', 'where', 'ret')
        self.assertEqual(cypher, 'match optional where ret')

        cypher = self.service.re_create_cypher_query('match', None, None, 'ret')
        self.assertEqual(cypher, 'match   ret')

        cypher = self.service.re_create_cypher_query('match', 'optional', None, 'ret')
        self.assertEqual(cypher, 'match optional  ret')

        cypher = self.service.re_create_cypher_query('match', None, 'where', 'ret')
        self.assertEqual(cypher, 'match  where ret')

    @patch('matcher.service.Matcher.send_matched_events_to_forwarder')
    def test_match_query_should_call_graph_db_api_with_proper_parameters(self, mocked_send_forwarder):
        query_id = 'query_id'
        vekg_stream = [1, 2, 3]
        self.service.query_matching[query_id] = {'cypher_query': 'some query'}
        match_ret = {'some-ret':[1]}
        self.service.graph_db_api = MagicMock()
        self.service.graph_db_api.match_query.return_value = match_ret

        self.service.match_query(query_id, vekg_stream)

        self.service.graph_db_api.add_query_vekg_window.assert_called_once_with(
            query_id,
            vekg_stream
        )
        self.service.graph_db_api.match_query.assert_called_once_with(
            query_id,
            'some query'
        )
        self.service.graph_db_api.clean_query_vekg_window.assert_called_once_with(
            query_id,
        )
        mocked_send_forwarder.assert_called_once_with(query_id, match_ret, vekg_stream)

    @patch('matcher.service.Matcher.send_matched_events_to_forwarder')
    def test_match_query_shouldnt_call_send_matched_events_to_forwarder_if_no_match(self, mocked_send_forwarder):
        query_id = 'query_id'
        vekg_stream = [1, 2, 3]
        self.service.query_matching[query_id] = {'cypher_query': 'some query'}
        match_ret = {}
        self.service.graph_db_api = MagicMock()
        self.service.graph_db_api.match_query.return_value = match_ret

        self.service.match_query(query_id, vekg_stream)

        self.service.graph_db_api.add_query_vekg_window.assert_called_once_with(
            query_id,
            vekg_stream
        )
        self.service.graph_db_api.match_query.assert_called_once_with(
            query_id,
            'some query'
        )
        self.service.graph_db_api.clean_query_vekg_window.assert_called_once_with(
            query_id,
        )
        self.assertFalse(mocked_send_forwarder.called)
