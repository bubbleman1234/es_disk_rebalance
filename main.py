from common import *

# Set Elasticsearch endpoint
elasticsearch_url = "http://10.232.167.78:9200"
limit_diskpercent = 50
shards_rotation = int(input("Number Shards Rotation: ") or "5")

# Fetch list of Elasticsearch nodes
def get_elasticsearch_nodes():
    url = f"{elasticsearch_url}/_cat/allocation?s=disk.percent:desc&h=node,ip,disk.used,disk.total,disk.percent,shards&format=JSON&bytes=b"
    result = send_request(url)
    return result

def check_hot_nodes(nodes):
    list_hotnodes = []
    for node in nodes:
        if int(node["disk.percent"]) > limit_diskpercent:
            list_hotnodes.append(node["node"])
    return list_hotnodes

def check_cold_nodes(nodes, hot_nodes):
    list_coldnodes = []
    copy_listnodes = nodes.copy()
    avg = find_average(nodes)
    # print(avg)
    for list_node in range(len(hot_nodes)):
        coldnode = None
        # print(copy_listnodes)
        for node in copy_listnodes:
            if not coldnode and node not in hot_nodes:
                coldnode = node
            elif (node["disk.percent"] < coldnode["disk.percent"]) and (node not in hot_nodes) and (int(node["disk.used"]) < avg):
                coldnode = node
        if coldnode is not None:
            list_coldnodes.append(coldnode)
            copy_listnodes.remove(coldnode)
            # print(list_coldnodes)
    # print(list_coldnodes)

def find_big_shards(list_nodes):
    url = f"{elasticsearch_url}/_cat/shards?s=store:desc&format=JSON&bytes=b"
    response = send_request(url)
    # print(shards_rotation)
    # print(response)
    result = dict()
    for node in list_nodes:
        list_swap_shard = list()
        for num_shard in range(shards_rotation):
            for each_shard in response:
                if each_shard["node"] == node and each_shard not in list_swap_shard:
                    list_swap_shard.append(each_shard)
                    break
        result[node] = list_swap_shard
    return result
    
def print_output(data, list_nodes):
    for each_node in list_nodes:
        list_shards = data[each_node]
        print("{0}\n=================".format(each_node))
        for each_shard in list_shards:
            shard_size = format_bytes(int(each_shard["store"]))
            print("{0}\t{1}\t{2}".format(each_shard["index"], each_shard["shard"], shard_size))


if __name__ == "__main__":
    nodes = get_elasticsearch_nodes()
    # print(nodes)
    if nodes:
        hot_nodes = check_hot_nodes(nodes)
        cold_nodes = check_cold_nodes(nodes, hot_nodes)
        big_shards = find_big_shards(hot_nodes)
        print_output(big_shards, hot_nodes)
    # print("All Nodes: {0}".format(nodes))
    # print("Hot Nodes: {0}".format(hot_nodes))