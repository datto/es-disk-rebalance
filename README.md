Tool for balancing disk usage on an Elasticsearch cluster.

Elasticsearch's built in rebalancing tries to balance index count, which can end with
certan nodes loaded with very large shards while other nodes hold small ones. This tool
swaps large shards with small shards to balance out the disk usage.

This script is intended to be ran automatically via cron or another periodic scheduler.
It may take a few runs to get a more optimal result.

```
usage: es-rebalance [-h] -u URL -b BOX_TYPE [-i ITERATIONS]
                    [-p SHARD_PERCENTAGE] [-P NODE_PERCENTAGE] [-v]
                    [--execute]

Disk-usage-based rebalancing tool for ElasticSearch.

Swaps large shards with small shards to balance disk usage between hosts.

Starts one set of swaps per run. Swaps will happen asynchronously after running this script. Multiple runs may be needed to fully balance.

This script is rack aware; it will not swap shards so that a primary and replica for the same index are on the same rack.

Example:

	es-rebalance -u localhost:9200 --box-type hot --iterations 50

optional arguments:
  -h, --help            show this help message and exit
  -u URL, --url URL     URL to cluster. Can be specified multiple times for
                        redundancy.
  -b BOX_TYPE, --box-type BOX_TYPE
                        Box type of nodes to rebalance. One of 'warm' or
                        'hot'.
  -i ITERATIONS, --iterations ITERATIONS
                        Maximum number of shards to exchange.
  -p SHARD_PERCENTAGE, --shard-percentage SHARD_PERCENTAGE
                        Don't exchange shards whose sizes are within this
                        percent of each other, to avoid swapping similar-sized
                        shards.
  -P NODE_PERCENTAGE, --node-percentage NODE_PERCENTAGE
                        Don't exchange between nodes whose sizes are within
                        this many percentage points of each other.
  -v, --verbose         Print debug logs.
  --execute             Run the plan. If not specified, will be a dry run.
```