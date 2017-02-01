Overview
========
Ratis replicated map is an implementation of a sorted map (think TreeMap) as
a replicated state machine. This is not under examples because it is intended
to be used in production where a simple in-memory map is sufficient to hold the
data. The data is fully cached in memory, but it is still durable since raft
log is used as a replicated log, and data is snapshotted periodically.


The replicated map (RMap) is not only the state machine implementation, but
all of the remaining code, including the client and querying capabilities which
is built on top of the other modules. In that sense, it is dog-fooding the ratis
library to implement an end-to-end solution for a replicated in-memory data store.
