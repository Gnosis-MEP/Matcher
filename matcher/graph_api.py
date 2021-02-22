from redisgraph import Node, Edge, Graph, Path


class RedisGraphDB():
    def __init__(self, redis_graph_db):
        self.redis_graph_db = redis_graph_db
        self.query_graphs = {}
        self.query_graphs_nodes = {}

    def _clean_identifier(self, identifier):
        if ' ' in identifier:
            return f'`{identifier}`'
        return identifier

    def add_vekg_nodes_to_graph(self, query_id, graph, event_id, node_tuples):
        for node_id, properties in node_tuples:
            node_kwargs = properties.copy()
            node_kwargs['event_id'] = event_id
            label = self._clean_identifier(node_kwargs.pop('label'))
            node_id = node_kwargs.pop('id')
            new_node = Node(node_id=node_id, label=label, properties=properties)
            graph.add_node(new_node)
            self.query_graphs_nodes[query_id][node_id] = new_node
        return graph

    def add_vekg_edges_to_graph(self, query_id, graph, edges_tuples):
        for edge_tuple in edges_tuples:
            node_u_id, node_v_id, properties = edge_tuple
            edge_kwargs = properties.copy()
            relation = self._clean_identifier(edge_kwargs.pop('relation'))
            node_u = self.query_graphs_nodes[query_id][node_u_id]
            node_v = self.query_graphs_nodes[query_id][node_v_id]
            edge = Edge(node_u, relation, node_v, properties=edge_kwargs)
            graph.add_edge(edge)
        return graph

    def add_same_frame_rel_edges(self, graph, event_id, vekg):
        """maybe best to put this relationship creation in another service
        but for now it will do
        """
        node_tuples = vekg.get('nodes', [])
        same_frames_relations = []
        for node_u in node_tuples:
            for node_v in node_tuples:
                if node_u[0] != node_v[0]:
                    relation = 'same_frame'
                    edge_tuple = (node_u[0], node_v[0], {'relation': relation})
                    same_frames_relations.append(edge_tuple)
        edge_tuples = vekg.get('edges', [])
        return edge_tuples + same_frames_relations

    def add_vekg_to_graph(self, query_id, graph, event_id, vekg):

        node_tuples = vekg.get('nodes', [])
        graph = self.add_vekg_nodes_to_graph(query_id, graph, event_id, node_tuples)

        edge_tuples = self.add_same_frame_rel_edges(graph, event_id, vekg)
        graph = self.add_vekg_edges_to_graph(query_id, graph, edge_tuples)

        return graph

    def add_query_vekg_window(self, query_id, vekg_window):
        query_graph_id = f'graph-{query_id}'
        query_graph = Graph(query_graph_id, self.redis_graph_db)
        self.query_graphs_nodes[query_id] = {}
        for event in vekg_window:
            event_id = event['id']
            vekg = event.get('vekg', {})
            query_graph = self.add_vekg_to_graph(query_id, query_graph, event_id, vekg)
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
            del self.query_graphs_nodes[query_id]
        except:
            pass
