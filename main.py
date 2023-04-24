import math
from common import *
from tabulate import tabulate
from colorama import Fore, Style

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
    list_coldnodes = list()
    copy_listnodes = nodes.copy()
    avg = find_average(nodes)
    
    for hot_node in hot_nodes:
        for node in copy_listnodes:
            if hot_node == node["node"]:
                copy_listnodes.remove(node)
    
    while len(list_coldnodes) < len(hot_nodes):
        coldnode = None
        for node in copy_listnodes:
            if (coldnode is None or (node["disk.percent"] < coldnode["disk.percent"] and int(node["disk.used"]) < avg and node not in hot_nodes)):
                coldnode = node
        if coldnode is not None:
            list_coldnodes.append(coldnode)
            copy_listnodes.remove(coldnode)
        else:
            break
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

def calculate_node_sizes(name_hot_nodes, name_cold_nodes, cold_nodes, big_shards, small_shards):
    shard_moves = dict()
    node_sizes = dict()
    # print(cold_nodes)
    
    for cold_node_name in name_cold_nodes:
        cold_node = cold_nodes[name_cold_nodes.index(cold_node_name)]
        hot_node_name = name_hot_nodes[cold_nodes.index(cold_node)]
        big_shards_list = big_shards[hot_node_name]
        
        increase_shard_size = sum([int(big_shard["store"]) for big_shard in big_shards_list])
        decrease_shard_size = sum([int(small_shard["store"]) for small_shard in small_shards[cold_node_name]])
        
        original_node_size = int(cold_node["disk.used"])
        percent_original_node_size = int(cold_node["disk.percent"])
        new_node_size = original_node_size - decrease_shard_size + increase_shard_size
        percent_new_node_size = math.ceil((new_node_size/int(cold_node["disk.total"]))*100)
        diff_percent = percent_new_node_size - percent_original_node_size
        
        if percent_new_node_size < limit_diskpercent:
            result = Fore.GREEN + "OK for swap" + Style.RESET_ALL
            result_code = "ok"
        else:
            result = Fore.RED + "Risk for swap" + Style.RESET_ALL
            result_code = "risk"
        
        
        shard_moves[cold_node_name] = [hot_node_name, [(big_shard["shard"], big_shard["index"], big_shard["store"]) for big_shard in big_shards_list], [(small_shard["shard"], small_shard["index"], small_shard["store"]) for small_shard in small_shards[cold_node_name]], result_code]
        node_sizes[cold_node_name] = [original_node_size, new_node_size, percent_original_node_size, percent_new_node_size, diff_percent, result]

    
    print("===== Summary Result =====")
    table_summary = [
        ["Cold Node Name", "Original Size", "New Size", "Original Size(%)", "New Size(%)", "%Increase", "Result"],
        *[
            [
                each_cold_node, format_bytes(node_sizes[each_cold_node][0]), format_bytes(node_sizes[each_cold_node][1]), 
                node_sizes[each_cold_node][2], node_sizes[each_cold_node][3], node_sizes[each_cold_node][4], node_sizes[each_cold_node][5]
            ]
            for each_cold_node in name_cold_nodes
        ]
    ]
    print(tabulate(table_summary, headers="firstrow", tablefmt="psql"))
    
    for cold_node_name in name_cold_nodes:
        # print(shard_moves)
        shard_move = shard_moves[cold_node_name]
        hot_node_name = shard_move[0]
        in_shards = shard_move[1]
        out_shards = shard_move[2]
        
        print("===== Detail Result Node {0} =====".format(cold_node_name))
        
        table_detail = [
            ["From node", "Index name", "Shard No.", "Size", "To node"],
            *[
                [hot_node_name, shard[1], shard[0], format_bytes(int(shard[2])), cold_node_name]
                for shard in in_shards
            ],
            *[
                [cold_node_name, shard[1], shard[0], format_bytes(int(shard[2])), hot_node_name]
                for shard in out_shards
            ]
        ]
        print(tabulate(table_detail, headers="firstrow", tablefmt="psql"))
    
    return shard_moves

def payload_move_shard(source_node, target_node, index_name, shard_number):
    payload = {
        "commands": [
            {
                "move": {
                    "index": index_name,
                    "shard": shard_number,
                    "from_node": source_node,
                    "to_node": target_node
                }
            }
        ]
    }
    return payload

def move_shard(list_move_shards):
    url = f"{elasticsearch_url}/_cluster/reroute"
    for key in list_move_shards.keys():
        hot_node = list_move_shards[key][0]
        in_shards = list_move_shards[key][1]
        out_shards = list_move_shards[key][2]
        can_swap = list_move_shards[key][3]
        
        if can_swap == 'ok':
            ## From Hot Nodes to Cold Nodes ##
            for shard in in_shards:
                payload = payload_move_shard(hot_node, key, shard[1], shard[0])
                response = send_request(url, "post", payload)
                if response:
                    print(Fore.GREEN + f"Shard {shard[0]} of index {shard[1]} moved from node {hot_node} to node {key}" + Style.RESET_ALL)
                else:
                    print(Fore.RED + f"Failed to move shard {shard[0]} of index {shard[1]} from node {hot_node} to node {key}." + Style.RESET_ALL)
            
            ## From Cold Nodes to Hot Nodes ##
            for shard in out_shards:
                payload = payload_move_shard(key, hot_node, shard[1], shard[0])
                response = send_request(url, "post", payload)
                if response:
                    print(Fore.GREEN + f"Shard {shard[0]} of index {shard[1]} moved from node {key} to node {hot_node}" + Style.RESET_ALL)
                else:
                    print(Fore.RED + f"Failed to move shard {shard[0]} of index {shard[1]} from node {key} to node {hot_node}." + Style.RESET_ALL)
                

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
        list_move_shards = calculate_node_sizes(name_hot_nodes, name_cold_nodes, cold_nodes, big_shards, small_shards)
        
        while(True):
            confirm = input("Are you confirm to swap shards? (Y/N): ")
            if confirm.upper() == "Y":
                move_shard(list_move_shards)
                break
            elif confirm.upper() == "N":
                exit()
        
        ## Output Section ##
        # print_output(big_shards, hot_nodes)
        # print_output(small_shards, cold_nodes)