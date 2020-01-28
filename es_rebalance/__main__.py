
from elasticsearch6 import Elasticsearch
from elasticsearch6.exceptions import TransportError
import argparse
import json
import logging
import pprint
import re
import statistics

from es_rebalance import *

LOG = logging.getLogger(__name__)

def main():
	parser = argparse.ArgumentParser(
		prog="es-rebalance",
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description="""\
Disk-usage-based rebalancing tool for ElasticSearch.

Swaps large shards with small shards to balance disk usage between hosts.

Starts one set of swaps per run. Swaps will happen asynchronously after running this script. Multiple runs may be needed to fully balance.

This script is rack aware; it will not swap shards so that a primary and replica for the same index are on the same rack.

Example:

	es-rebalance -u localhost:9200 --box-type hot --iterations 50
""")
	parser.add_argument("-u", "--url", required=True, action="append",
		help="URL to cluster. Can be specified multiple times for redundancy.")
	parser.add_argument("-b", "--box-type", required=True,
		help="Box type of nodes to rebalance. One of 'warm' or 'hot'.")
	parser.add_argument("-i", "--iterations", type=int, default=10,
		help="Maximum number of shards to exchange.")
	parser.add_argument("-p", "--shard-percentage", type=float, default=90,
		help="Don't exchange shards whose sizes are within this percent of each other, to avoid swapping similar-sized shards.")
	parser.add_argument("-P", "--node-percentage", type=float, default=10,
		help="Don't exchange between nodes whose sizes are within this many percentage points of each other.")
	parser.add_argument("-v", "--verbose", action="store_true",
		help="Print debug logs.")
	parser.add_argument("--execute", action="store_true",
		help="Run the plan. If not specified, will be a dry run.")
	
	args = parser.parse_args()
	
	logging.basicConfig(level=logging.INFO)
	if args.verbose:
		LOG.setLevel(logging.DEBUG)
	
	es = Elasticsearch(args.url)
	
	plan = Plan(es, args.box_type, args.shard_percentage, args.node_percentage)
	for i in range(args.iterations):
		if not plan.plan_step():
			LOG.warn("Could not move anything, stopping early after %d iteration(s)", i+1)
			break
	
	plan.exec(dry_run=not args.execute)
	if not args.execute:
		LOG.warn("Finished dry run. Use `--execute` to run for real.")

if __name__ == "__main__":
	main()
