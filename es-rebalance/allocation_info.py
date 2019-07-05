
from collections import namedtuple
import re

AllocationInfo = namedtuple("AllocationInfo", [
	"node_host",
	"node_ip",
	"num_shards",
	"disk_indices",
	"disk_used",
	"disk_avail",
	"disk_total",
	"disk_percent",
])
ALLOCATION_RE = re.compile(r"""^
	(?P<num_shards>[0-9]+)\ +
	(?P<disk_indices>[0-9]+)\ +
	(?P<disk_used>[0-9]+)\ +
	(?P<disk_avail>[0-9]+)\ +
	(?P<disk_total>[0-9]+)\ +
	(?P<disk_percent>[0-9]+)\ +
	[^ ]+\ +
	(?P<node_ip>[^ ]+)\ +
	(?P<node_host>[^ ]+)\s*
$""", re.X)

def get_allocation_info(es):
	for line in es.cat.allocation(bytes="b").splitlines():
		match = ALLOCATION_RE.match(line)
		yield AllocationInfo(
			num_shards=int(match.group("num_shards")),
			disk_indices=int(match.group("disk_indices")),
			disk_used=int(match.group("disk_used")),
			disk_avail=int(match.group("disk_avail")),
			disk_total=int(match.group("disk_total")),
			disk_percent=int(match.group("disk_percent")),
			node_ip=match.group("node_ip"),
			node_host=match.group("node_host"),
		)