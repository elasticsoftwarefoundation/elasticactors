Elastic Actors
=============

Persistent Stateful Actor System

### Current released version

![CI](https://github.com/elasticsoftwarefoundation/elasticactors/workflows/CI/badge.svg)
[![License: Apache 2](https://img.shields.io/badge/LICENSE-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Maven Central](https://img.shields.io/maven-central/v/org.elasticsoftwarefoundation.elasticactors/elasticactors-parent.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.elasticsoftwarefoundation.elasticactors%22)

### Add Elastic Actors to your Project

For convenience and guaranteed compatiblity across versions, it's advisable to use our BOM in 
your `dependencyManagement` section:
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
            <artifactId>elasticactors-bom</artifactId>
            <version>${elasticactors.version}</version>
            <scope>import</scope>
            <type>pom</type>
        </dependency>
    </dependencies>
</dependencyManagement>
```

Minimal dependency:
```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-api</artifactId>
    <version>${elasticactors.version}</version> <!-- Can be ommitted when using the BOM -->
</dependency>
```
Convenient base classes inclusing a Jackson 2 based serialization framework:
```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-base</artifactId>
    <version>${elasticactors.version}</version> <!-- Can be ommitted when using the BOM -->
</dependency>
```

### Example code

Some example code (in java)

```java
@Message(serializationFramework = JacksonSerializationFramework.class,durable = true)
public class Greeting {
    private final String who;

    @JsonCreator
    public Greeting(@JsonProperty("who") String who) {
        this.who = who;
    }

    public String getWho() {
        return who;
    }
}

@Actor(stateClass = StringState.class,serializationFramework = JacksonSerializationFramework.class)
public class GreetingActor extends TypedActor<Greeting> {
    @Override
    public void onReceive(ActorRef sender, Greeting message) throws Exception {
        System.out.println("Hello " + message.getWho());
    }
}

TestActorSystem testActorSystem = new TestActorSystem();
testActorSystem.initialize();

ActorSystem actorSystem = testActorSystem.getActorSystem();
ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class);
greeter.tell(new Greeting("Joost van de Wijgerd"),null);

testActorSystem.destroy();
```

### Upgrading Elastic Actors

Unless specified, Elastic Actors versions are backwards compatible at runtime and at the wire level.

The following exceptions apply:

* **1.x - 4.0.x** to **4.1.x** or later:
  * Changes to the shard-to-node distribution algorithm require the actor system to be completely 
    destroyed or scaled down to 1 node before deploying the new version.

### Basic configuration

The Actor System can be configured with a minimal YAML configuration file.
Keys noted with an exclamation point mean they must not be changed after the Actor System has been
put in production. They affect the communication between actors and changing them will cause
the system not to communicate properly anymore. 

The Actor System name is used in the formation of the Actor's path.\
The shard hash seed is used to determine in which shard a persitent actor will be placed.\
Changing either of them will cause the system not to find existing actors upon restart.

Example: An actor that was in `actor://test.clusterName/test/shards/11/actorId` would now be 
found in `actor://newName.clusterName/test/shards/23/actorId`. Changing the name would change
the beginning of the path, causing issues with persisted `ActorRef` objects. Changing the
shard hashing seed may change the shard in which the actor resides. Therefore, it would not 
be found in the persistence layer anymore, and it would not be able to receive messages.

```yaml
---
# The name of this Actor System.
# This MUST NOT be changed after the system has been deployed for the first time.
name: test

# The number of shards to use for persistent actors in this Actor System.
# This MUST NOT be changed after the system has been deployed for the first time.
shards: 8


## Optional keys for performance tuning. 
# Refer to ActorSystemConfiguration for more details.

# The number of queues per node.
# This affects the parallelism level of temporary and service actors.
# Default: 1.
queuesPerNode: 8

# The number of queues per shard.
# You can increase this in order to get more paralellism without resharding the system.
# A change on this must be accompained by a change on the remote actor system configuration
# of other actor systems that communicate with this one.
# It's recommended to fully destroy and redeploy the system, and remote systems that send 
# messages to this one, if this changes.
# Default: 1
queuesPerShard: 2

# The hashing seed used for allocating actors to shards.
# It's better to use a good prime number for better distribution.
# This MUST NOT be changed after the system has been deployed for the first time.
# Default: 0.
shardHashSeed: 0

# The hashing seed used for sending messages to the queues, when using more than one
# queue per shard or per node.
# It's better to use a good prime number for better load balancing.
# A change on this must be accompained by a change on the remote actor system configuration
# of other actor systems that communicate with this one.
# It's recommended to fully destroy and redeploy the system, and remote systems that send 
# messages to this one, if this changes.
# If using multiple queues per node, but only one queue per shard, restarting only
# this actor system will suffice.
# Default: 53.
multiQueueHashSeed: 53

# The hashing seed used for distributing shards among nodes.
# It's better to use a good prime number for better load balancing.
# It's recommended to fully destroy and redeploy the system if this changes; or scale it down
# to a single node before restarting it.
# If two running nodes have different values, they might end up listening to the same shard
# at the same time, which must be avoided.
# Default: 53
shardDistributionHashSeed: 53


## Remote actor systems configuration.
# In order for Actor Systems to communicate with one another, they must know
# how to find each other and how to map actor IDs to shards and queues on 
# the other system.
remoteActorSystems:
  - clusterName: test2.elasticsoftware.org
    name: default
    shards: 8
    #queuesPerShard: 2
    #multiQueueHashSeed: 53
  - clusterName: test3.elasticsoftware.org
    name: default
    shards: 8
    #queuesPerShard: 3
    #multiQueueHashSeed: 53
    
## Actor Configuration properties
# Any other objects will be read as properties for a given actor type. 

# The general format is:
actor.fully.qualified.class.name:
  keys: values
  
# These are accessible through ActorSystemConfiguration. Example:
org.elasticactors.http.actors.HttpService:
  listenPort: 8080
org.elasticactors.test.TestActor:
  pushConfigurations:
    - clientId: 123282
      keystoreResource: classpath:test_1.p12
      keystorePassword: bladiebla
    - clientId: 193382
      keystoreResource: classpath:test_1.p12
      keystorePassword: bladiebla
org.elasticsoftware.elasticactors.test.TestActorFullName:
  pushConfigurations:
    - clientId: 123282
      keystoreResource: classpath:test_1.p12
      keystorePassword: bladiebla
    - clientId: 193382
      keystoreResource: classpath:test_1.p12
      keystorePassword: bladiebla
```

### Configuration properties

Elastic Actors scans actor and message classes under the packages defined in the file 
`classpath:META-INF/elasticactors.properties`. This file is required and contains only the following
property:

```properties
basePackage=org.elasticsoftware.elasticactors.base
```

### Configuration properties

These properties can be set per-environment using any property source accepted by Spring.

```properties
## Basic configuration

# Elastic Actors cluster name
ea.cluster=cluster.name

# This node's name (usually the hostname)
ea.node.id=node-name

# This node's IP address or host name
ea.node.address=localhost

# Elastic Actors config file location. 
# Default: "classpath:ea-default.yaml"
ea.node.config.location=file:/etc/config.yaml


## Jackson serialization framework

# Toggles the usage of the Jackson Afterburner module
ea.base.useAfterburner=false

# Additional packages for scanning, if any, for registering subtypes using Jackson
ea.scan.packages=org.elasticsoftware.elasticactors


## Caching and tuning

# Maximum number of node-bound actors that can exist simultaneously in the node at any given time.
# This affects the maximum number of temporary actors that can be used.
# Default: 10240
ea.nodeCache.maximumSize=10240

# Maximum number of cached Persistent Actors.
# Default: 10240
ea.shardCache.maximumSize=10240

# Maximum number of actor references that can be cached.
# Default: 10240
ea.actorRefCache.maximumSize=10240

# Number of executor threads used for actor message handling.
# Default: the host's number of processors multiplied by 3
ea.actorExecutor.workerCount=32

# Toggle the usage of Disruptors for the actor thread bound executor instead of blocking queues.
# Default: false
ea.actorExecutor.useDisruptor=false

# Number of executor threads used for message queues.
# Default: the host's number of processors multiplied by 3
ea.queueExecutor.workerCount=16

# Toggle the usage of Disruptors for the queue thread bound executor instead of blocking queues.
# Default: false
ea.queueExecutor.useDisruptor=false

# Number of executor threads used for the asynchronous actor state update executor.
# Default: the host's number of processors multiplied by 3
ea.asyncUpdateExecutor.workerCount=10


## Shoal clustering library

# Address and port numbers for node discovery.
ea.cluster.discovery.nodes=tcp://localhost:9090,tcp://localhost:9091,tcp://localhost:9092

# Port for connection.
# Default: 9090
ea.node.port=9090


## K8s clustering library

# Name of the StatefulSet
ea.cluster.kubernetes.statefulsetName=actor-system-set

# Namespace in which the actor system's StatfulSet is deployed.
# Default: default
ea.cluster.kubernetes.namespace=default

# Toggles the usage of the number of desired replicas, rather than the number of 
# current replicas, when determining how many actor system nodes there currently are.
# It's recommended to turn this off to minimize downtime, but this was historically 
# the default strategy, so it remains enabled unless turned off.
# Default: true
ea.cluster.kubernetes.useDesiredReplicas=true


## RabbitMQ messaging layer

# RabbitMQ hosts
ea.rabbitmq.hosts=127.0.0.1

# RabbitMQ user name.
# Default: guest
ea.rabbitmq.username=guest

# RabbitMQ password.
# Default: guest
ea.rabbitmq.password=guest

# RabbitMQ ACK type.
# Default: DIRECT
ea.rabbitmq.ack=ASYNC

# RabbitMQ thread model.
# Default: sc
ea.rabbitmq.threadmodel=cpt

# RabbitMQ prefetch count.
# Default: 0
ea.rabbitmq.prefetchCount=100


## Persistent Actor Repository layer

# Minimum size (in bytes) for which a serialized actor state will be compressed with LZ4
ea.persistentActorRepository.compressionThreshold=512


## Cassandra backplane

# Cassandra hosts.
# Default: localhost
ea.cassandra.hosts=127.0.0.1

# Cassandra cluster name.
# Default: ElasticActorsCluster
ea.cassandra.cluster=ElasticActorsCluster

# Cassandra keyspace.
# Default: "ElasticActors"
ea.cassandra.keyspace="ElasticActors"

# Port for connection to Cassandra.
# Default: 9042
ea.cassandra.port=9042

# Delay for retrying downed hosts, in seconds.
# Default: 1
ea.cassandra.retryDownedHostsDelayInSeconds=1

# Maximum active connections per host.
# Only backplane-cassandra and backplane-cassandra2.
# Default: the host's number of processors multiplied by 3
ea.cassandra.maxActive=12

# Batch size for batch updates
# Default: 20
ea.asyncUpdateExecutor.batchSize=20

# Whether or not to optimize v1 batches
# Only backplane-cassandra2 (implemented) and backplane-cassandra4 (but not implemented yet).
# Default: true
ea.asyncUpdateExecutor.optimizedV1Batches=true

# Let HFactory manage the cluster.
# It seems that there are issues with the CassandraHostRetryService and retrying downed hosts.
# If we don't let the HFactory manage the cluster, then CassandraHostRetryService doesn't try to
# be smart about finding out if a host was removed from the ring, and so it will keep on retrying
# all configured hosts (and ultimately fail-back when the host comes back online).
# Only backplane-cassandra.
# Default: true
ea.cassandra.hfactory.manageCluster=true


## Metrics and Logging

# Toggles metrics for messages in node queues
# Default: false
ea.metrics.node.messaging.enabled=false

# Configures the threshold for message delivery (total time spent in broker + local queues)
# in microseconds
# Default: 0
ea.metrics.node.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds
# Default: 0
ea.metrics.node.messaging.handling.warn.threshold=0

# Configures the threshold for message delivery (total time spent in broker + local queues)
# in microseconds
# Default: 0
ea.metrics.shard.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds. 
# Default: 0
ea.metrics.shard.messaging.handling.warn.threshold=0

# Configures the threshold for actor state serialization. 
# Default: 0.
ea.metrics.shard.serialization.warn.threshold=0

# Toggles logging for messages in shard queues. 
# Default: false
ea.logging.shard.messaging.enabled=false

# Toggles logging for messages in node queues. 
# Default: false
ea.logging.node.messaging.enabled=false

# Optional LogFeature overrides for specific message types.
# The value is a list of comma-separated features
ea.logging.messages.overrides.{{class_name}}=TIMING,CONTENTS

# The maximum number of characters when logging message contents. 
# Default: 5000
ea.logging.messages.maxLength=5000

# If set to true, transient messages will use the toString() method 
# instead of being serialized for logging purposes. 
# Default: false
ea.logging.messages.transient.useToString=false
```

### System properties

The following properties must be provided as system properties, if changing them is desired.

```properties
# Toggles serialization caching. 
# Default: false
ea.serializationCache.enabled=false

# Toggles deserialization caching. 
# Default: false
ea.deserializationCache.enable=false

# Changes the logging level used when logging unhandled message types in MethodActor.
# Default: WARN
ea.logging.messages.unhandled.level=WARN
```

### Class loading cache

Elastic Actors dynamically loads classes during message handling.
That hasn't been a performance issue so far, but if you're making use 
of the logging facilities above, the number of times a class is dynamically 
loaded can increase. Since these operations can introduce some level of 
contention due to synchronization, a class loading cache is available
if that happens to impact the performance of your application. Just add 
the following dependency to your build:

```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-caching</artifactId>
    <scope>runtime</scope>
</dependency>
```

### Tracing

Elastic Actors has a distributed tracing implementation based on the [OpenTracing](https://opentracing.io) specification.\
It supports trace ID propagation and custom baggage, including propagation. 

There is currently no support for exporting traces or span sampling.
The code is highly optimized, however, and there is little overhead in using it.

Messages also carry data about where they were created.

If in an Actor Context, the actor's type and ID will be used. If not, it's possible to define
a custom creation context. \
For example, when receiving a request from a web controller, you
can set the current creation context which will get injected into any messages sent during the 
handling of that request.

In order to use distributed tracing, include the following dependency in your project:

```xml
<dependency>
    <groupId>
        <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
        <artifactId>elasticactors-tracing</artifactId>
        <scope>compile</scope> <!-- or runtime if you don't need the Spring bits-->
    </groupId>
</dependency>
```

In order to instrument asynchronous executor beans automatically, import `TracingConfiguration` to
your Spring configuration. This includes a bean post-processor that wraps those facilities in ones
that can automatically propagate the traces from Elastic Actors.

`MessagingContextManager` is the main class responsible for managing trace context data.\
It allows you to put a set of trace and creation contexts into scope. It also adds the trace
and creation contexts, as well as some message handling information, to the logging library's 
MDC (Mapped Diagnostic Context) through Slf4j. 

#### Performance impact of tracing on applications using Log4j2

If running a web application while using Log4j2 for logging, using tracing can generate
a big amount of garbage objects due to Log4j2 using a different implementation of the map
that backs the MDC. In order to avoid performance issues, run your application with the following
system properties set to:

```properties
log4j2.is.webapp=false
log4j2.garbagefree.threadContextMap=true
```


