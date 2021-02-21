from redisgraph import Node, Edge, Graph, Path


class RedisGraphDB():
    def __init__(self, redis_graph_db):
        self.redis_graph_db = redis_graph_db
        self.query_graphs = {}

    def _clean_identifier(self, identifier):
        if ' ' in identifier:
            return f'`{identifier}`'
        return identifier

    def add_vekg_nodes_to_graph(self, graph, event_id, note_tuples):
        for node_id, properties in note_tuples:
            node_kwargs = properties.copy()
            node_kwargs['event_id'] = event_id
            label = self._clean_identifier(node_kwargs.pop('label'))
            new_node = Node(label=label, properties=properties)
            graph.add_node(new_node)
        return graph

    def add_vekg_edges_to_graph(self, graph, edges_tuples):
        for edge in edges_tuples:
            node_u, node_v, properties = edge
            edges_kwargs = properties.copy()
            relation = self._clean_identifier(edges_kwargs.pop('relation'))
            graph.add_edge(node_u, node_v, relation=relation, properties=properties)
        return graph

    def add_vekg_to_graph(self, graph, event_id, vekg):
        note_tuples = vekg.get('nodes', [])
        graph = self.add_vekg_nodes_to_graph(graph, event_id, note_tuples)
        edge_tuples = vekg.get('edges', [])
        graph = self.add_vekg_edges_to_graph(graph, edge_tuples)
        return graph

    def add_query_vekg_window(self, query_id, vekg_window):
        query_graph = Graph(query_id, self.redis_graph_db)
        for event in vekg_window:
            event_id = event['id']
            vekg = event.get('vekg', {})
            query_graph = self.add_vekg_to_graph(query_graph, event_id, vekg)
        query_graph.commit()
        self.query_graphs[query_id] = query_graph
        return self.query_graphs[query_id]

    def match_query(self, query_id, cypher_query):
        query_graph = self.query_graphs[query_id]

        result = query_graph.query(cypher_query)
        return result.result_set

    def clean_query_vekg_window(self, query_id):
        try:
            self.query_graphs[query_id].delete()
            del self.query_graphs[query_id]
        except:
            pass
