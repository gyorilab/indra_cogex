This document describes notes on deploying the INDRA CoGEx neo4j instance
on an AWS EC2 server.

General
-------
The server is running Ubunut 20.04.

All neo4j commands need to use sudo. Though some of the commands don't error
if you forget to use sudo, they don't have the desired effect.

Logs can be found in /var/log/neo4j, there is the main neo4j.log and
the more verbose debug.log.


Memory issues
-------------
When increasing the size of the DB we started noticing out of memory errors.

neo4j.log:
```
Exception in thread "neo4j.Scheduler-1"
Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "neo4j.StorageMaintenance-11"
...
Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "neo4j.BoltNetworkIO-58"
```

debug.log:
```
2021-12-19 22:44:48.949+0000 ERROR [o.n.b.v.m.BoltRequestMessageReaderV41] Unable to send error back to the client Message has already been started, index: 589
java.lang.IllegalStateException: Message has already been started, index: 589
        at org.neo4j.bolt.packstream.ChunkedOutput.beginMessage(ChunkedOutput.java:85)
        at org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3.packCompleteMessageOrFail(BoltResponseMessageWriterV3.java:107)
        at org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3.write(BoltResponseMessageWriterV3.java:81)
        at org.neo4j.bolt.v41.messaging.BoltResponseMessageWriterV41.write(BoltResponseMessageWriterV41.java:63)
        at org.neo4j.bolt.v3.messaging.MessageProcessingHandler.publishError(MessageProcessingHandler.java:145)
        at org.neo4j.bolt.v3.messaging.MessageProcessingHandler.onFinish(MessageProcessingHandler.java:105)
        at org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine.after(AbstractBoltStateMachine.java:131)
        at org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine.process(AbstractBoltStateMachine.java:96)
        at org.neo4j.bolt.messaging.BoltRequestMessageReader.lambda$doRead$1(BoltRequestMessageReader.java:90)
        at org.neo4j.bolt.runtime.DefaultBoltConnection.lambda$enqueue$0(DefaultBoltConnection.java:148)
        at org.neo4j.bolt.runtime.DefaultBoltConnection.processNextBatchInternal(DefaultBoltConnection.java:237)
        at org.neo4j.bolt.runtime.DefaultBoltConnection.processNextBatch(DefaultBoltConnection.java:172)
        at org.neo4j.bolt.runtime.DefaultBoltConnection.processNextBatch(DefaultBoltConnection.java:162)
        at org.neo4j.bolt.runtime.scheduling.ExecutorBoltScheduler.executeBatch(ExecutorBoltScheduler.java:246)
        at org.neo4j.bolt.runtime.scheduling.ExecutorBoltScheduler.lambda$scheduleBatchOrHandleError$3(ExecutorBoltScheduler.java:229)
        at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1700)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
        at java.base/java.lang.Thread.run(Thread.java:829)
        Suppressed: java.lang.OutOfMemoryError: Java heap space
```

Context from forums:
https://community.neo4j.com/t/neo4j-crashing-regularly-because-of-outofmemoryerror/26843

Documentation of memory configuration: https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/

The `sudo neo4j-admin memrec` command is useful for inspecting the current distribution of data and indexes. This recommends:
```
dbms.memory.heap.initial_size=31g
dbms.memory.heap.max_size=31g
dbms.memory.pagecache.size=202900m

# It is also recommended turning out-of-memory errors into full crashes,
# instead of allowing a partially crashed database to continue running:
#dbms.jvm.additional=-XX:+ExitOnOutOfMemoryError
```
to be set in the configuration file.

After an out of memory error, the service also wouldn't stop, as below:
```
Stopping Neo4j......................................................................................................................... failed to stop
Neo4j (pid 2084681) took more than 120 seconds to stop.
Please see /var/log/neo4j/neo4j.log for details.
```

Context from forums: https://community.neo4j.com/t/neo4j-pid-774-took-more-than-120-seconds-to-stop/3073

Recommendation is to kill the process manually, i.e., `sudo kill -9 2084681`
for the above example.

Open files limit
----------------
There is a persistent warning e.g., when starting the neo4j service saying
```
WARNING: Max 1024 open files allowed, minimum of 40000 recommended. See the Neo4j manual.
```

Forum context: https://community.neo4j.com/t/warning-max-1024-open-files-allowed-minimum-of-40000-recommended-see-the-neo4j-manual/3679

It seems like adding a file /etc/security/limits.d/neo4j.conf
with content like
```
* soft nofile 40000
* hard nofile 40000
```
would help but it might require a server restart.
