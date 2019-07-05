
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

def get_shards(es):
	for shard in es.cat.shards(format="json", bytes="b"):
		yield Shard(
			index_name=shard["index"],
			shard_id=int(shard["shard"]),
			is_primary=shard["prirep"] == "p",
			status=shard["state"],
			num_docs=int(shard["docs"]),
			size=int(shard["store"]),
			node_ip=shard["ip"],
			node_host=shard["node"],
		)
