
from collections import namedtuple
from elasticsearch6 import Elasticsearch
from elasticsearch6.exceptions import TransportError
import json
import logging
import pprint
import re
import statistics

LOG = logging.getLogger(__name__)

__all__ = [
	"NodeInfo",
	"Shard",
	"Plan",
]

class NodeInfo:
	"""
	Info about an ES node
	
	Fields:
	
	* name: str
	* ip: str
	* rack: str, rack name of the node for replication awareness
	* capacity: int, in bytes
	* shards: list of Shard, usually sorted largest-first. After editing call resort.
	* used: int, in bytes. Calculated by resort.
	"""
	def __init__(self, name, ip, rack, capacity, shards):
		# Immutable properties
		self.name = name
		self.ip = ip
		self.rack = rack
		self.capacity = capacity
		
		# Mutable properties
		# `shards` and `used` are updated during planning.
		# `shards` is sorted by size (store)
		self.shards = shards
		self.used = None
		self.resort()
	
	@property
	def fraction_used(self):
		"""
		Computes the fractional disk usage - a float ranging from 0 to 1 with 0 being
		empty and 1 being full.
		"""
		return self.used / self.capacity
	
	def resort(self):
		"""
		Sorts the `shards` list by size and updates `used`. Call after modifying `shards`.
		"""
		self.shards.sort(key=lambda shard: shard.store, reverse=True)
		self.used = sum(shard.store for shard in self.shards)

# Shard class
# Namedtuple since it does not need any methods
Shard = namedtuple("Shard", [
	"index", # str, index name
	"shard", # int, stard index
	"prirep", # str, either "p" for primaries or "r" for replicas
	"store", # int, size of shard in bytes
	"can_move", # bool, can we move this shard? Can't be moved if relocating, in a bad state, etc
])

# Regex for parsing relocating nodes
RELOCATING_RE = re.compile(r"^([^ ]+) -> [^ ]+ [^ ]+ ([^ ]+)$")

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

class Plan:
	"""
	Main class for planning shard swaps
	
	Fields:
	
	* es: ES connection
	* shard_fraction_threshold: float from 0..1, don't consider exchanges of shards whose sizes are within
	  this fraction of each other.
	* node_fraction_threshold: float from 0..1, don't consider exchanges between nodes whose percent used
	  are within this fraction of each other.
	* operations: list of dicts compatible with ES's reroute command. Pending operations.
	* nodes_by_size: list of NodeInfo, sorted by percent used largest-first.
	* moved_stards: set of Shard that have already been moved.
	"""
	def __init__(self, es, box_type, shard_percentage_threshold, node_percentage_threshold):
		self.es = es
		self.shard_fraction_threshold = 1 - shard_percentage_threshold / 100
		self.node_fraction_threshold = node_percentage_threshold / 100
		
		# Grab node and shard info from ES
		raw_alloc_infos = es.cat.allocation(format="json", bytes="b")
		raw_shards = es.cat.shards(format="json", bytes="b")
		raw_nodes = dict((node["name"], node) for node in es.nodes.info(format="json")["nodes"].values())
		
		# Make NodeInfo structs from nodes
		nodes = {}
		for alloc_info in raw_alloc_infos:
			if alloc_info["node"] == "UNASSIGNED":
				continue
			if raw_nodes[alloc_info["node"]]["attributes"].get("box_type") != box_type:
				continue
			nodes[alloc_info["node"]] = NodeInfo(
				name=alloc_info["node"],
				ip=alloc_info["ip"],
				rack=raw_nodes[alloc_info["node"]]["attributes"]["rack_id"],
				capacity=int(alloc_info["disk.total"]),
				shards=[],
			)
		
		# Fill out NodeInfo's shards list
		for shard in raw_shards:
			if shard["state"] == "RELOCATING":
				match = RELOCATING_RE.match(shard["node"])
				shard_nodes = (match.group(1), match.group(2))
			else:
				shard_nodes = (shard["node"],)
			
			can_move = shard["state"] == "STARTED"
			if not can_move:
				LOG.warn("Won't be able to move shard %s on host %s, state is %s",
					shard, shard_nodes, shard["state"])
			
			for node_name in shard_nodes:
				if node_name not in nodes:
					continue
				nodes[node_name].shards.append(Shard(
					index=shard["index"],
					shard=int(shard["shard"]),
					prirep=shard["prirep"],
					store=int(shard["store"]) if shard["store"] is not None else 0,
					can_move=can_move,
				))
		
		self.operations = []
		self.nodes_by_size = list(nodes.values())
		self.moved_shards = set()
		self._sort()
	
	def _sort(self):
		"""
		Resorts all nodes and the `nodes_by_size` list.
		Call after modifying a node's shards list.
		"""
		for node in self.nodes_by_size:
			node.resort()
		self.nodes_by_size.sort(
			key=lambda node: node.used / node.capacity,
			reverse=True)
	
	def plan_step(self):
		"""
		Plans a single exchange.
		
		Call this once for every exchange you want to do.
		
		Returns true if a shard was able to be exchanged, or false if
		no shards are able to exchange anymore.
		"""
		current_pvariance = self.percent_used_variance()
		
		for big_node, big_shard in self.find_big_shards():
			for small_node, small_shard in self.find_small_shards(big_node):
				# Is the "small" shard actually the smaller of the two?
				if small_shard.store >= big_shard.store:
					continue
				
				# Can we even move this node?
				if not self.can_exchange_shards(big_node, big_shard, small_node, small_shard):
					continue
				
				# Make sure moving this actually makes things more even
				exchanged_pvariance = self.percent_used_variance(
					exclude_shards=((big_node, big_shard), (small_node, small_shard)),
					include_shards=((small_node, big_shard), (big_node, small_shard)),
				)
				if exchanged_pvariance >= current_pvariance:
					continue
				
				self.plan_exchange(big_node, big_shard, small_node, small_shard)
				return True
		return False
	
	def find_big_shards(self):
		"""
		Yields large shards that ought to be exchanged, with the highest-priority shards first.
		
		Prioritize moving shards off the most full hosts, with the largest shards going first.
		"""
		for node in self.nodes_by_size:
			if abs(self.nodes_by_size[-1].fraction_used - node.fraction_used) < self.node_fraction_threshold:
				# Big node has a similar percent used than all of the smaller nodes, so there's
				# no way we can exchange anything. Stop.
				break
			
			for shard in node.shards:
				if (shard.index, shard.shard) in self.moved_shards:
					continue
				if not shard.can_move:
					continue
				yield node, shard
	
	def find_small_shards(self, big_node):
		"""
		Yields small shards that ought to be exchanged, with the highest-priority shards first.
		
		Prioritizes the opposite way that find_big_shards does: starts with smallest shards on most free hosts.
		"""
		for node in reversed(self.nodes_by_size):
			if node is big_node:
				# We've met up with the big node. All other nodes will be bigger than it,
				# so there's no point in continuing
				break
			
			for shard in reversed(node.shards):
				if (shard.index, shard.shard) in self.moved_shards:
					continue
				if not shard.can_move:
					continue
				yield node, shard
	
	def can_exchange_shards(self, node1, node1_shard, node2, node2_shard):
		"""
		Checks if two shards can be exchanged, according to the setup rules for the plan, the node's available
		disk space, and the one-replica-per-rack ES rule.
		"""
		# Can't swap a node with itself
		if node1 is node2:
			return False
		
		# Is the swap large enough to be worth it?
		if node1_shard.store > node2_shard.store:
			size_fraction = node2_shard.store / node1_shard.store
		else:
			size_fraction = node1_shard.store / node2_shard.store
		if size_fraction > self.shard_fraction_threshold:
			LOG.debug("Not worth swapping: shard %s/%s/%s (%s) with shard %s/%s/%s (%s)",
				node1_shard.index, node1_shard.shard, node1_shard.prirep,
				format_bytes(node1_shard.store),
				node2_shard.index, node2_shard.shard, node2_shard.prirep,
				format_bytes(node2_shard.store),
			)
			return False
		
		# Are the two nodes too similar in disk utilization?
		if abs(node1.used / node1.capacity - node2.used / node2.capacity) < self.node_fraction_threshold:
			return False
		
		# Will the shards fit?
		if node1.used + node2_shard.store > node1.capacity or node2.used + node1_shard.store > node2.capacity:
			LOG.debug("Too big: %d >? {} and/or %d >? %d",
				node1.used + node2_shard.store, node1.capacity, node2.used + node1_shard.store, node2.capacity)
			return False
		
		# Check if node1_shard can be moved to node2 without violating the one shard per rack rule
		for node in self.nodes_by_size:
			# Ignore ourself, otherwise we'll conflict with ourself
			if node is node1:
				continue
			# If the node we're checking is not on the same rack as the
			# node we're moving to, it won't conflict
			if node.rack != node2.rack:
				continue
			# Check if any shards in the node share the index and shard number.
			# If found, a shard replica/primary is already located on the destination node's
			# rack and we can't move.
			for checking_shard in node.shards:
				if node1_shard.index == checking_shard.index and \
					node1_shard.shard == checking_shard.shard:
					LOG.debug("Rack conflict: moving shard %s/%s/%s to node %s would conflict with shard %s/%s/%s on node %s",
						node1_shard.index, node1_shard.shard, node1_shard.prirep,
						node1.name,
						checking_shard.index, checking_shard.shard, checking_shard.prirep,
						node.name,
					)
					return False
		# Same but for node2_shard to node1
		for node in self.nodes_by_size:
			if node is node2:
				continue
			if node.rack != node1.rack:
				continue
			for checking_shard in node.shards:
				if node2_shard.index == checking_shard.index and \
					node2_shard.shard == checking_shard.shard:
					LOG.debug("Rack conflict: moving shard %s/%s/%s to node %s would conflict with shard %s/%s/%s on node %s",
						node2_shard.index, node2_shard.shard, node2_shard.prirep,
						node2.name,
						checking_shard.index, checking_shard.shard, checking_shard.prirep,
						node.name,
					)
					return False
		
		return True
	
	def plan_exchange(self, node1, node1_shard, node2, node2_shard):
		"""
		Adds move operations to exchange the two shards. Also updates the node's shards list.
		"""
		LOG.info("Exchanging shard %s/%s/%s (%s) on node %s (%.2f%%) with %s/%s/%s (%s) on node %s (%.2f%%)",
			node1_shard.index, node1_shard.shard, node1_shard.prirep, format_bytes(node1_shard.store),
			node1.name, sum(node.store for node in node1.shards) * 100 / node1.capacity,
			node2_shard.index, node2_shard.shard, node2_shard.prirep, format_bytes(node2_shard.store),
			node2.name, sum(node.store for node in node2.shards) * 100 / node2.capacity,
		)
		
		self.operations.append({
			"move": {
				"index": node1_shard.index,
				"shard": node1_shard.shard,
				"from_node": node1.name,
				"to_node": node2.name,
			},
		})
		self.operations.append({
			"move": {
				"index": node2_shard.index,
				"shard": node2_shard.shard,
				"from_node": node2.name,
				"to_node": node1.name,
			},
		})
		node1.shards.remove(node1_shard)
		node1.shards.append(node2_shard)
		node2.shards.remove(node2_shard)
		node2.shards.append(node1_shard)
		self.moved_shards.add((node1_shard.index, node1_shard.shard))
		self.moved_shards.add((node2_shard.index, node2_shard.shard))
		self._sort()
	
	def percent_used_variance(self, exclude_shards=[], include_shards=[]):
		"""
		Gets the variance of the percent disk used
		
		Optionally exclude or include shards in the computation, for seeing if a
		configuration is better without actually committing to it
		"""
		def percentage(node):
			the_sum = node.used
			for exclude_node, exclude_shard in exclude_shards:
				if exclude_node is node:
					the_sum -= exclude_shard.store
			for include_node, include_shard in include_shards:
				if include_node is node:
					the_sum += include_shard.store
			
			return the_sum / node.capacity
		return statistics.pvariance(percentage(node) for node in self.nodes_by_size)
	
	def exec(self, dry_run=True):
		"""
		Executes the operations enqueued with `plan_step`.
		
		This submits the operations to Elasticsearch, which will execute them asynchronously.
		As such, this method returns quickly and without waiting for the moves to finish.
		"""
		try:
			self.es.cluster.reroute(body={"commands": self.operations}, dry_run=dry_run)
		except TransportError as err:
			LOG.error("ES error info: \n%s", pprint.pformat(err.info))
			raise
		self.operations.clear()
