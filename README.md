cassandra-opstools
==================

Generic tools and scripts to help operating Cassandra cluster

spcassandra-abortrepairs:
  Stops ongoing anti-entropy sessions on the local Cassandra host

spcassandra-autobalance:
  Automatically redistributes the tokens in a cluster so they are evenly
  distributed. Tries to move as few tokens as possible to achieve this.
  In a multi-DC setup, all datacenters must have the same number of nodes.
  Only useful when not using vnodes.

spcassandra-dsnitch:
  Outputs the score the Cassandra snitch has for every peer.

spcassandra-generate-repairs:
  Generates "nodetool repair" commands that repairs an entire cluster
  with small token ranges.

spcassandra-repairstats:
  Scans the Cassandra system log and displays readable statistics
  of finished and running repairs.

spcassandra-tombstones:
  Scans a sstable and prints number of tombstones for each partition.

spcassandra-truncate[all]hints:
  Truncates (all) hints on localhost toward the specified hosts.
