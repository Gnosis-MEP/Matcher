import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer


class Matcher(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 graph_db_api,
                 forwarder_stream_key,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(Matcher, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

        self.forwarder_stream_key = forwarder_stream_key
        self.forwarder_stream = self.stream_factory.create(key=forwarder_stream_key, stype='streamOnly')

        self.graph_db_api = graph_db_api
        self.query_matching = {}

    def prepare_events_for_forwarder(self, query_id, match_ret, vekg_stream):
        """For now will just repass the vekg stream to the forwarder
        But in the future it would be nice to use the match_ret return
        to only send that specific data in the format of VEKG, or filtered VEKG
        """
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'vekg_stream': vekg_stream,
            'query_id': query_id,
        }
        return new_event_data

    def send_matched_events_to_forwarder(self, query_id, match_ret, vekg_stream):
        new_event_data = self.prepare_events_for_forwarder(query_id, match_ret, vekg_stream)
        self.logger.debug(f'Sending Matched VEKG Stream to Forwarder: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.forwarder_stream)

    @timer_logger
    def match_query(self, query_id, vekg_stream):
        try:
            cypher_query = self.query_matching[query_id]['cypher_query']
            self.graph_db_api.add_query_vekg_window(query_id, vekg_stream)
            match_ret = self.graph_db_api.match_query(query_id, cypher_query)
            if match_ret != []:
                self.send_matched_events_to_forwarder(query_id, match_ret, vekg_stream)
        finally:
            self.graph_db_api.clean_query_vekg_window(query_id)

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(Matcher, self).process_data_event(event_data, json_msg):
            return False

        query_id = event_data['query_id']
        vekg_stream = event_data['vekg_stream']
        self.match_query(query_id, vekg_stream)

    def re_create_cypher_query(self, match, optional_match, where, ret):
        optional_match = optional_match if optional_match is not None else ''
        where = where if where is not None else ''
        cypher_query = ' '.join([match, optional_match, where, ret])
        return cypher_query

    def add_query_matching_action(self, query_id, match, optional_match, where, ret):
        cypher_query = self.re_create_cypher_query(match, optional_match, where, ret)
        self.query_matching[query_id] = {
            'match': match,
            'optional_match': optional_match,
            'where': where,
            'ret': ret,
            'cypher_query': cypher_query
        }

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(Matcher, self).process_event_type(event_type, event_data, json_msg):
            return False
        if event_type == 'QueryCreated':
            parsed_query = event_data['parsed_query']
            self.add_query_matching_action(
                query_id=event_data['query_id'],
                match=parsed_query['match'],
                optional_match=parsed_query['optional_match'],
                where=parsed_query['where'],
                ret=parsed_query['ret']
            )

    def log_state(self):
        super(Matcher, self).log_state()
        self._log_dict('Query Matching', self.query_matching)

    def run(self):
        super(Matcher, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
