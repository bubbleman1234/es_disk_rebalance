import requests
import json

def find_average(nodes):
    total_usage = 0
    for each_node in nodes:
        total_usage += int(each_node["disk.used"])
    
    avg = round(total_usage/len(nodes))
    return avg

def send_request(url, method="get", payload=""):
    if method.lower() == "get":
        response = requests.get(url)
    elif method.lower() == "post":
        response = requests.post(url, json=payload)
    elif method.lower() == "put":
        response = requests.put(url)
    elif method.lower() == "delete":
        response = requests.delete(url)
    else:
        return None
    
    if response.status_code == 200:
        response_text = response.text
        response_json = json.loads(response_text)
        return response_json
    else:
        print("!!!!! Sending Request Error !!!!!")
        print("Error Code: {0}".format(response.status_code))
        print("Error Detail:")
        print(response.text)
        return None

def list_el_shard(list_nodes, response, shards_rotation):
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

def list_name_from_dict(datadict):
    result = list()
    for each_dict in datadict:
        result.append(each_dict["node"])
    return result

def format_bytes(num_bytes):
	"""
	Formats a number into human-friendly byte units (KiB, MiB, etc)
	"""
	if num_bytes >= 1024*1024*1024*1024:
		return "%.2fTiB" % (num_bytes / (1024*1024*1024*1024))
	if num_bytes >= 1024*1024*1024:
		return "%.2fGiB" % (num_bytes / (1024*1024*1024))
	if num_bytes >= 1024*1024:
		return "%.2fMiB" % (num_bytes / (1024*1024))
	if num_bytes >= 1024:
		return "%.2fKiB" % (num_bytes / (1024))
	return "%dB" % num_bytes