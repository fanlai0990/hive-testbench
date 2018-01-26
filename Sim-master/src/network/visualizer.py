import networkx as nx
import matplotlib.pyplot as plt


class Visualizer(object):
    def __init__(self, graph=None, gml_filename=None, links_kv=None, nodes_kv=None):
        self.gml_filename = gml_filename
        self.G = graph
        self.links_kv = links_kv
        self.nodes_kv = nodes_kv
        self.node_pos = {}
        self.node_power = {}

    def visualize_network(self, graph_name):
        if self.G is not None:
            self.visualize_network_from_graph(graph_name)
        elif self.gml_filename is not None:
            self.visualize_network_from_gml(graph_name)
        elif self.links_kv is not None and self.links_kv is not None:
            self.visualize_network_from_kv(graph_name)
        else:
            raise

    def visualize_network_from_graph(self, graph_name):
        # locations for nodes and node labels
        longitude_list = nx.get_node_attributes(self.G, 'Longitude')
        latitude_list = nx.get_node_attributes(self.G, 'Latitude')
        location = {}
        label_pos = {}
        for key in self.G.nodes():
            location[key] = (float(longitude_list[key]), float(latitude_list[key]))
            label_pos[key] = (float(longitude_list[key]), float(latitude_list[key]) + 0.5)
        # draw graph
        nx.draw_networkx_nodes(self.G, pos=location, node_size=50, node_color='y')
        nx.draw_networkx_edges(self.G, location, edgelist=self.G.edges(data=True))
        nx.draw_networkx_labels(self.G, pos=label_pos, font_size=6, font_color='b')
        plt.savefig(graph_name, dpi=800)
        self.G.clear()

    def visualize_network_from_gml(self, graph_name):
        self.G = nx.read_gml(self.gml_filename)
        self.visualize_network_from_graph(graph_name)

    def visualize_network_from_kv(self, graph_name):
        self.G = nx.Graph()
        for node_key, node_info in self.nodes_kv.items():
            self.G.add_node(node_key)
            self.node_pos[node_key] = node_info["pos"]
            self.node_power[node_key] = node_info["power"]

        for link_key, link_bandwidth in self.links_kv.items():
            self.G.add_edge(link_key[0], link_key[1], bandwidth=link_bandwidth)
        self.G.width = [d['bandwidth'] / 10 for (u, v, d) in self.G.edges(data=True)]

        # nx.draw_networkx_nodes(self.G)
        nx.draw_networkx_nodes(self.G, self.node_pos, node_size=100, node_color=[self.node_power[v] for v in self.G])
        nx.draw_networkx_edges(self.G, self.node_pos, edgelist=self.G.edges(data=True), width=self.G.width)

        # for labeling outside the node
        offset = 0.3
        labels_node_pos = {}
        for key in self.node_pos.keys():
            x, y = self.node_pos[key]
            labels_node_pos[key] = (x + offset, y)
        nx.draw_networkx_labels(self.G, pos=labels_node_pos, fontsize=10)

        plt.savefig(graph_name)
        self.G.clear()
