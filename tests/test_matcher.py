from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from matcher.service import Matcher

from matcher.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
    FORWARDER_STREAM_KEY,
)


class TestMatcher(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'forwarder_stream_key': FORWARDER_STREAM_KEY,
        'graph_db_api': lambda x: (),
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = Matcher
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    @patch('matcher.service.Matcher.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'id': 1,
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_action.__name__ = 'process_action'

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action=action, event_data=event_data, json_msg=msg_tuple[1])

    @patch('matcher.service.Matcher.add_query_matching_action')
    def test_process_action_should_call_add_query_matching_with_proper_parameters(self, mocked_add_query_m):
        event_data = {
            'id': 1,
            'action': 'addQueryMatching',
            'query_id': 'query_id1',
            'match': 'MATCH (c1:Car) AND (c2:Car {color:"white"})',
            'where': 'WHERE c1.color = "blue" or c1.color = "black"',
            'ret': 'RETURN c1, c2',
        }
        action = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_action(action, event_data, json_msg)
        mocked_add_query_m.assert_called_once_with(
            query_id=event_data['query_id'],
            match=event_data['match'],
            where=event_data['where'],
            ret=event_data['ret']
        )

    def test_add_query_matching_action_should_update_datastructure(self):
        query_id = 'query_id1'
        match = 'MATCH (c1:Car) AND (c2:Car {color:"white"})'
        where = 'WHERE c1.color = "blue" or c1.color = "black"'
        ret = 'RETURN c1, c2'
        self.service.add_query_matching_action(query_id, match, where, ret)
        self.assertIn(query_id, self.service.query_matching.keys())
        self.assertDictEqual(
            {
                'match': match,
                'where': where,
                'ret': ret,
                'cypher_query': (
                    'MATCH (c1:Car) AND (c2:Car {color:"white"})'
                    ' WHERE c1.color = "blue" or c1.color = "black"'
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
        cypher = self.service.re_create_cypher_query('match', 'where', 'ret')
        self.assertEqual(cypher, 'match where ret')

        cypher = self.service.re_create_cypher_query('match', None, 'ret')
        self.assertEqual(cypher, 'match  ret')

    @patch('matcher.service.Matcher.send_matched_events_to_forwarder')
    def test_match_query_should_call_graph_db_api_with_proper_parameters(self, mocked_send_forwarder):
        query_id = 'query_id'
        vekg_stream = [1, 2, 3]
        self.service.query_matching[query_id] = {'cypher_query': 'some query'}
        match_ret = ['some-ret']
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
        match_ret = []
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
