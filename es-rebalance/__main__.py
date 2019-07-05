
from collections import namedtuple
from elasticsearch6 import Elasticsearch
import argparse
import json
import pprint

from .allocation_info import get_allocation_info
from .shards import get_shards

def main():
	parser = argparse.ArgumentParser(description="")
	parser.add_argument("-u", "--url", required=True, action="append",
		help="URL to cluster. Can be specified multiple times")
	
	args = parser.parse_args()
	
	es = Elasticsearch(args.url)
	
	shards = list(get_shards(es))
	
	if not shards:
		print("No shards found.")
		return
	
	shards.sort(key = lambda shard: (shard.index_name, shard.shard_id, shard.is_primary))
	for shard in shards:
		print(shard)
	
	allocation_info = list(get_allocation_info(es))
	allocation_info.sort(key=lambda n: n.node_host)
	for info in allocation_info:
		print(info)

if __name__ == "__main__":
	main()
