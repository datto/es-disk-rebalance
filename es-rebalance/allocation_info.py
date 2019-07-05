
from collections import namedtuple

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

def get_allocation_info(es):
	for alloc in es.cat.allocation(format="json", bytes="b"):
		yield AllocationInfo(
			num_shards=int(alloc["shards"]),
			disk_indices=int(alloc["disk.indices"]),
			disk_used=int(alloc["disk.used"]),
			disk_avail=int(alloc["disk.avail"]),
			disk_total=int(alloc["disk.total"]),
			disk_percent=int(alloc["disk.percent"]),
			node_ip=alloc["ip"],
			node_host=alloc["node"],
		)
