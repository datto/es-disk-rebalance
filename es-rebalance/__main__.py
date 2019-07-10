
from collections import namedtuple
from elasticsearch6 import Elasticsearch
from elasticsearch6.exceptions import TransportError
from sortedcontainers import SortedSet
import argparse
import json
import logging
import pprint
import re
import statistics

LOG = logging.getLogger(__name__)

NodeInfo = namedtuple("NodeInfo", [
	"name",
	"ip",
	"rack",
	"capacity",
	"shards",
])
Shard = namedtuple("Shard", [
	"index",
	"shard",
	"prirep",
	"store",
	"can_move",
])

RELOCATING_RE = re.compile(r"^([^ ]+) -> [^ ]+ [^ ]+ ([^ ]+)$")

def formate_bytes(num_bytes):
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
	def __init__(self, es, box_type, size_percent_threshold):
		self.es = es
		self.size_fraction_threshold = 1 - size_percent_threshold / 100
		raw_alloc_infos = es.cat.allocation(format="json", bytes="b")
		raw_shards = es.cat.shards(format="json", bytes="b")
		raw_nodes = dict((node["name"], node) for node in es.nodes.info(format="json")["nodes"].values())
		
		nodes = {}
		for alloc_info in raw_alloc_infos:
			if raw_nodes[alloc_info["node"]]["attributes"].get("box_type") != box_type:
				continue
			nodes[alloc_info["node"]] = NodeInfo(
				name=alloc_info["node"],
				ip=alloc_info["ip"],
				rack=raw_nodes[alloc_info["node"]]["attributes"]["rack_id"],
				capacity=int(alloc_info["disk.total"]),
				shards=[],
			)
		
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
					store=int(shard["store"]),
					can_move=can_move,
				))
		
		self.operations = []
		self.nodes_by_size = list(nodes.values())
		self.moved_shards = set()
		self.shards_by_size = None
		self._sort()
	
	def _sort(self):
		self.nodes_by_size.sort(
			key=lambda node: sum(shard.store for shard in node.shards) / node.capacity,
			reverse=True)
		for node in self.nodes_by_size:
			node.shards.sort(key=lambda shard: shard.store, reverse=True)
		#self.shards_by_size = sorted(
		#	((host, shard) for host in self.nodes_by_size for shard in host.shards),
		#	key=lambda tup: tup[1].store
		#)
	
	def plan_step(self):
		current_pvariance = self.percent_used_variance()
		
		for big_node, big_shard in self.find_big_shards():
			for small_node, small_shard in self.find_small_shards():
				if small_shard.store >= big_shard.store:
					continue
				
				# Can we even move this node?
				if not self.can_exchange_shards(big_node, big_shard, small_node, small_shard):
					continue
				
				# Make sure moving this actually makes things more even
				exchanged_pvariance = self.percent_used_variance(
					exclude_shards=(big_shard, small_shard),
					include_shards=((small_node, big_shard), (big_node, small_shard)),
				)
				if exchanged_pvariance >= current_pvariance:
					continue
				
				self.plan_exchange(big_node, big_shard, small_node, small_shard)
				return True
		return False
	
	def find_big_shards(self):
		"Yields shards that ought to be moved, in decreasing priority"
		for node in self.nodes_by_size:
			for shard in node.shards:
				if (shard.index, shard.shard) in self.moved_shards:
					continue
				if not shard.can_move:
					continue
				yield node, shard
	
	def find_small_shards(self):
		for node in reversed(self.nodes_by_size):
			for shard in reversed(node.shards):
				if (shard.index, shard.shard) in self.moved_shards:
					continue
				if not shard.can_move:
					continue
				yield node, shard
	
	def can_exchange_shards(self, node1, node1_shard, node2, node2_shard):
		if node1 is node2:
			return False
		
		# Is the swap large enough to be worth it?
		if node1_shard.store > node2_shard.store:
			size_fraction = node2_shard.store / node1_shard.store
		else:
			size_fraction = node1_shard.store / node2_shard.store
		if size_fraction > self.size_fraction_threshold:
			LOG.debug("Not worth swapping: shard %s/%s/%s (%s) with shard %s/%s/%s (%s)",
				node1_shard.index, node1_shard.shard, node1_shard.prirep,
				formate_bytes(node1_shard.store),
				node2_shard.index, node2_shard.shard, node2_shard.prirep,
				formate_bytes(node2_shard.store),
			)
			return False
		
		# Will the shards fit?
		node1_used = sum(shard.store for shard in node1.shards) \
			+ node2_shard.store
		node2_used = sum(shard.store for shard in node2.shards) \
			+ node1_shard.store
		if node1_used > node1.capacity or node2_used > node2.capacity:
			LOG.debug("Too big: %d >? {} and/or %d >? %d", node1_used, node1.capacity, node2_used, node2.capacity)
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
		LOG.info("Exchanging shard %s/%s/%s (%s) on node %s (%.2f%%) with %s/%s/%s (%s) on node %s (%.2f%%)",
			node1_shard.index, node1_shard.shard, node1_shard.prirep, formate_bytes(node1_shard.store),
			node1.name, sum(node.store for node in node1.shards) * 100 / node1.capacity,
			node2_shard.index, node2_shard.shard, node2_shard.prirep, formate_bytes(node2_shard.store),
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
			the_sum = sum((shard.store if shard not in exclude_shards else 0) for shard in node.shards)
			for include_node, include_shard in include_shards:
				if include_node is node:
					the_sum += include_shard.store
			return the_sum / node.capacity
		return statistics.pvariance(percentage(node) for node in self.nodes_by_size)
	
	def exec(self, dry_run=True):
		try:
			self.es.cluster.reroute(body={"commands": self.operations}, dry_run=dry_run)
		except TransportError as err:
			LOG.error("ES error info: \n%s", pprint.pformat(err.info))
			raise
		self.operations.clear()


def main():
	parser = argparse.ArgumentParser(description="")
	parser.add_argument("-u", "--url", required=True, action="append",
		help="URL to cluster. Can be specified multiple times")
	parser.add_argument("-b", "--box-type", required=True,
		help="Box type of nodes to rebalance. One of 'cold' or 'hot'")
	parser.add_argument("-i", "--iterations", type=int, default=10,
		help="Number of shards to exchange")
	parser.add_argument("-p", "--size-percentage", type=float, default=90,
		help="Reject exchanges of shards whose sizes are within this percent of each other, to avoid swapping large shards around.")
	parser.add_argument("-v", "--verbose", action="store_true",
		help="Print debug logs")
	parser.add_argument("--execute", action="store_true",
		help="Run the plan. If not specified, will be a dry run.")
	
	args = parser.parse_args()
	
	logging.basicConfig(level=logging.INFO)
	if args.verbose:
		LOG.setLevel(logging.DEBUG)
	
	es = Elasticsearch(args.url)
	
	plan = Plan(es, args.box_type, args.size_percentage)
	for i in range(args.iterations):
		if not plan.plan_step():
			LOG.warn("Could not move anything, stopping early after %d iteration(s)", i+1)
			break
	#pprint.pprint(plan.operations)
	
	plan.exec(dry_run=not args.execute)
	if not args.execute:
		LOG.warn("Finished dry run. Use `--execute` to run for real.")

if __name__ == "__main__":
	main()
