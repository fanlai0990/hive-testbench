import networkx as nx
import sys
import os

ORANGE_BW = 100000  # 100 Gbps
BLUE_BW = 9953  # 9.953 Gbps
GREEN_BW = 2488  # 2.488 Gbps
RED_BW = 622  # 622.08 Mbps


def main():
    fo = open(os.path.dirname(__file__) + '/../../data/raw_graph/ntt.txt', 'rw+')
    lines = fo.readlines()
    fo.close()

    node_loc_dict = dict()  # {name: (latitude, longitude)}
    total_bw_dict = dict()  # {(start, end): total_bw}
    ginpop_set = set()
    dc_set = set()
    ppp_set = set()

    lid = 0
    while lid < len(lines):
        if len(lines[lid]) == 0 or lines[lid].startswith('//'):
            lid += 1

        elif lines[lid].startswith('var '):
            node_loc_str = lines[lid].replace('var ', '')
            node_loc_str = node_loc_str.replace('= new google.maps.LatLng(', '')
            node_loc_str = node_loc_str.replace(',', ' ')
            node_loc_str = node_loc_str.replace(');', '')

            splits = node_loc_str.split()
            name = splits[0]
            latitude = round(float(splits[1]), 2)
            longitude = round(float(splits[2]), 2)
            node_loc_dict[name] = (latitude, longitude)

            lid += 1

        elif lines[lid].startswith('routes'):
            link_str = lines[lid].replace('routes.push({', '')
            link_str = link_str.replace('\'', '')
            link_str = link_str.replace(':', '')
            link_str = link_str.replace(',', '')
            link_str = link_str.replace('});', '')

            splits = link_str.split()
            i = 0
            start_node = str()
            end_node = str()
            bw_sum = 0
            while i < len(splits):
                if splits[i] == 'start':
                    start_node = splits[i + 1]
                    i += 1
                elif splits[i] == 'finish':
                    end_node = splits[i + 1]
                    i += 1
                elif splits[i] == 'blue':
                    bw_sum += BLUE_BW * int(splits[i + 1])
                    i += 1
                elif splits[i] == 'orange':
                    bw_sum += ORANGE_BW * int(splits[i + 1])
                    i += 1
                elif splits[i] == 'green':
                    bw_sum += GREEN_BW * int(splits[i + 1])
                    i += 1
                elif splits[i] == 'reg':
                    bw_sum += RED_BW * int(splits[i + 1])
                    i += 1
                else:
                    i += 1
            total_bw_dict[(start_node, end_node)] = bw_sum

            lid += 1

        elif lines[lid].startswith('ginpoparray'):
            ginpop_str = lines[lid].replace('ginpoparray[', '')
            ginpop_str = ginpop_str.replace('] = addGINPOP(', ' ')
            ginpop_str = ginpop_str.replace(',', ' ')
            ginpop_str = ginpop_str.replace(');', ' ')

            splits = ginpop_str.split()
            ginpop_set.add(splits[-4])

            lid += 1

        elif lines[lid].startswith('dcarray'):
            dc_str = lines[lid].replace('dcarray.push(addDatacenter(', '')
            dc_str = dc_str.replace(',', ' ')
            dc_str = dc_str.replace('));', '')

            splits = dc_str.split()
            dc_set.add(splits[-4])
            print 'dc', splits[-4]
            lid += 1

        elif lines[lid].startswith('ppparray'):
            ppp_str = lines[lid].replace('ppparray.push(addPPP(', '')
            ppp_str = ppp_str.replace(',', ' ')

            splits = ppp_str.split()
            ppp_set.add(splits[-4])

            lid += 1

        else:
            lid += 1

    for node_name, loc_tuple in node_loc_dict.items():
        if node_name not in ginpop_set and node_name not in dc_set and node_name not in ppp_set:
            del node_loc_dict[node_name]

    # generate gml format
    graph = nx.Graph(network='NTT')
    for node_name, loc_tuple in node_loc_dict.items():
        graph.add_node(node_name, latitude=loc_tuple[0], longitude=loc_tuple[1])
        graph.node[node_name]['ginpop'] = node_name in ginpop_set
        graph.node[node_name]['dc'] = node_name in dc_set
        graph.node[node_name]['ppp'] = node_name in ppp_set
    for link_tuple, bw in total_bw_dict.items():
        if bw != 0:
            graph.add_edge(link_tuple[0], link_tuple[1], bandwidth=bw)

    nx.write_gml(graph, os.path.dirname(__file__) + '/../../data/gml/ntt_new.gml')


if __name__ == "__main__":
    main()
