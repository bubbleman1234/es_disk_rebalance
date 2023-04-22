from common import *
from tabulate import tabulate

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
            list_hotnodes.append(node)
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
    return list_coldnodes

def find_big_shards(list_nodes):
    url = f"{elasticsearch_url}/_cat/shards?s=store:desc&format=JSON&bytes=b"
    response = send_request(url)
    # print(shards_rotation)
    # print(response)
    nodes_name = list_name_from_dict(list_nodes)
    result = list_el_shard(nodes_name, response, shards_rotation)
    return result

def find_small_shards(list_nodes):
    url = f"{elasticsearch_url}/_cat/shards?s=store:asc&format=JSON&bytes=b"
    response = send_request(url)
    nodes_name = list_name_from_dict(list_nodes)
    result = list_el_shard(nodes_name, response, shards_rotation)
    return result

def print_output(data, list_nodes):
    for each_node in list_nodes:
        list_shards = data[each_node["node"]]
        print("{0}\n=================".format(each_node["node"]))
        for each_shard in list_shards:
            shard_size = format_bytes(int(each_shard["store"]))
            print("{0}\t{1}\t{2}".format(each_shard["index"], each_shard["shard"], shard_size))

def size_after_relocate(name_hot_nodes, name_cold_nodes, hot_nodes, cold_nodes, big_shards, small_shards):
    # print(big_shards)
    print(small_shards)
    index = 0
    for node in cold_nodes:
        print(node)
        bigshards_list = big_shards[name_hot_nodes[index]]
        increase_shards_size = 0
        decrease_shards_size  = 0
        for bigshard in bigshards_list:
            increase_shards_size += int(bigshard["store"])
        for smallshard in small_shards[node["node"]]:
            print(smallshard)
            decrease_shards_size += int(smallshard["store"])
        print("Increase: {0}".format(increase_shards_size))
        print("Decrease: {0}".format(decrease_shards_size))
        new_node_size = int(node["disk.used"]) - decrease_shards_size + increase_shards_size
        print("{0}:\t{1}".format(node["node"], new_node_size))
        index += 1

def calculate_node_sizes(name_hot_nodes, name_cold_nodes, cold_nodes, big_shards, small_shards):
    shard_moves = dict()
    node_sizes = dict()
    # print(big_shards)
    
    for cold_node_name in name_cold_nodes:
        cold_node = cold_nodes[name_cold_nodes.index(cold_node_name)]
        hot_node_name = name_hot_nodes[cold_nodes.index(cold_node)]
        big_shards_list = big_shards[hot_node_name]
        
        increase_shard_size = sum([int(big_shard["store"]) for big_shard in big_shards_list])
        decrease_shard_size = sum([int(small_shard["store"]) for small_shard in small_shards[cold_node_name]])
        
        original_node_size = int(cold_node["disk.used"])
        new_node_size = original_node_size - decrease_shard_size + increase_shard_size
        
        shard_moves[cold_node_name] = (hot_node_name, [(big_shard["shard"], big_shard["index"], big_shard["store"]) for big_shard in big_shards_list], [(small_shard["shard"], small_shard["index"], small_shard["store"]) for small_shard in small_shards[cold_node_name]])
        node_sizes[cold_node_name] = new_node_size
        
    for cold_node_name in name_cold_nodes:
        # print(shard_moves)
        shard_move = shard_moves[cold_node_name]
        hot_node_name = shard_move[0]
        in_shards = shard_move[1]
        out_shards = shard_move[2]
        
        table = [["Index name", "Shard", "Size", "From node"],
             *[[shard[1], shard[0], format_bytes(int(shard[2])), hot_node_name] for shard in in_shards]]
        
        print("Node {0}:".format(cold_node_name))
        print("Original size: {0}".format(format_bytes(int(cold_nodes[name_cold_nodes.index(cold_node_name)]["disk.used"]))))
        print("New size: {0}".format(format_bytes(node_sizes[cold_node_name])))
        print("Shards moving in:")
        print(tabulate(table, headers="firstrow", tablefmt="psql"))
        
        table = [["Index name", "Shard", "Size", "To node"],
             *[[shard[1], shard[0], format_bytes(int(shard[2])), hot_node_name] for shard in out_shards]]
        print("Shards moving out:")
        print(tabulate(table, headers="firstrow", tablefmt="psql"))
        
        print("==================================")
    
    return shard_moves

if __name__ == "__main__":
    nodes = get_elasticsearch_nodes()
    if nodes:
        hot_nodes = check_hot_nodes(nodes)
        name_hot_nodes = list_name_from_dict(hot_nodes)
        cold_nodes = check_cold_nodes(nodes, name_hot_nodes)
        name_cold_nodes = list_name_from_dict(cold_nodes)
        big_shards = find_big_shards(hot_nodes)
        small_shards = find_small_shards(cold_nodes)
        
        # status_check = size_after_relocate(name_hot_nodes, name_cold_nodes, hot_nodes, cold_nodes, big_shards, small_shards)
        status_check = calculate_node_sizes(name_hot_nodes, name_cold_nodes, cold_nodes, big_shards, small_shards)
        
        ## Output Section ##
        # print_output(big_shards, hot_nodes)
        # print_output(small_shards, cold_nodes)