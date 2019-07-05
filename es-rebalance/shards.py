
from collections import namedtuple
import re

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
SHARD_RE = re.compile(r"""^
	(?P<index_name>[^ ]+)\ +
	(?P<shard_id>[0-9]+)\ +
	(?P<is_primary>[pr])\ +
	(?P<status>[^ ]+)\ +
	(?P<num_docs>[0-9]+)\ +
	(?P<size>[0-9]+)\ +
	(?P<node_ip>[^ ]+)\ +
	(?P<node_host>[^\s]+)\s*
$""", re.X)

def get_shards(es):
	for line in es.cat.shards(bytes="b").splitlines():
		match = SHARD_RE.match(line)
		yield Shard(
			match.group("index_name"),
			int(match.group("shard_id")),
			match.group("is_primary") == "p",
			match.group("status"),
			int(match.group("num_docs")),
			int(match.group("size")),
			match.group("node_ip"),
			match.group("node_host"),
		)
