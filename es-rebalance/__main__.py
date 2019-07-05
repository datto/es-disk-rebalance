
from elasticsearch6 import Elasticsearch
import argparse
import re
from collections import namedtuple

Shard = namedtuple("Shard", [
	"index_name",
	"shard_id",
	"is_primary",
	"status",
	"num_docs",
	"size",
	"node_ip",
	"node_host",
])
SHARD_RE = re.compile(r"^([^ ]+) +([0-9]+) +([pr]) +([^ ]+) +([0-9]+) +([0-9]+\.?[0-9]*[a-z]*) +([^ ]+) +([^\s]+)\s*$")

def main():
	parser = argparse.ArgumentParser(description="")
	parser.add_argument("-u", "--url", required=True, action="append",
		help="URL to cluster. Can be specified multiple times")
	
	args = parser.parse_args()
	
	es = Elasticsearch(args.url)
	
	shards = []
	for line in es.cat.shards().splitlines():
		match = SHARD_RE.match(line)
		shards.append(Shard(
			match.group(1),
			int(match.group(2)),
			match.group(3) == "p",
			match.group(4),
			int(match.group(5)),
			match.group(6),
			match.group(7),
			match.group(8),
		))
	
	if not shards:
		print("No shards found.")
		return
	
	shards.sort(key = lambda shard: (shard.name, shard.unknown_int, shard.is_primary))
	for shard in shards:
		print(shard)

if __name__ == "__main__":
	main()
