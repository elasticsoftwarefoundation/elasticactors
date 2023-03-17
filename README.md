Elastic Actors
=============

Persistent Stateful Actor System


## Current released version (v6.x Java 8 compatible)

[![CI](https://github.com/elasticsoftwarefoundation/elasticactors/actions/workflows/maven-v6.yml/badge.svg)](https://github.com/elasticsoftwarefoundation/elasticactors/actions/workflows/maven-v6.yml)
[![License: Apache 2](https://img.shields.io/badge/LICENSE-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Maven Central](https://img.shields.io/maven-central/v/org.elasticsoftwarefoundation.elasticactors/elasticactors-parent.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.elasticsoftwarefoundation.elasticactors%22)


## Add Elastic Actors to your Project

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
Convenient base classes, including a Jackson-based serialization framework:
```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-base</artifactId>
    <version>${elasticactors.version}</version> <!-- Can be ommitted when using the BOM -->
</dependency>
```


## Example code

Some example code (in Java):

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

public class Main {
    public static void main(String[] args) throws Exception {
      TestActorSystem testActorSystem = new TestActorSystem();
      testActorSystem.initialize();

      ActorSystem actorSystem = testActorSystem.getActorSystem();
      ActorRef greeter = actorSystem.actorOf("greeter", GreetingActor.class);
      greeter.tell(new Greeting("Joost van de Wijgerd"), null);

      testActorSystem.destroy();
    }
}
```


## Upgrading Elastic Actors

Unless specified, Elastic Actors versions are backwards compatible at runtime and at the wire level.

The following exceptions apply:

* **1.x - 4.x** to **5.x** or later:
  * Changes to the shard-to-node distribution algorithm require the actor system to be completely 
    destroyed or scaled down to 1 node before deploying the new version.
  * The tracing module was split between its basic implementation and an optional module to integrate
    it with the logging framework. See [how to add trace information to logs in the section below](#adding-trace-information-to-logs).
* **2.5 - 5.1** to **5.2** or later:
  * The tracing module now integrates with Spring without the need to explicitly import the `TracingConfiguration` class.
    See [how to instrument Spring beans in the section below](#instrumenting-spring-beans).
  * Metrics and Logging settings are now only enabled for undeliverable messages and messages that use 
    the reactive streams protocol (such as Subscriptions) if explicitly configured.
  * Configuration keys for indexing with Elasticsearch have been changed from `actor.indexing.*` 
    to `ea.indexing.*` in order for it to use the same format as other modules. 
* **1.x - 5.x** to **6.0** or later:
  * Elastic Actors 6.0 introduces timeouts for Temporary Actors. If your application has long-lived 
    Temporary Actors, make sure to configure the timeout properties, so they suit your use-case.
  * A major bug in the BUFFERED RabbitMQ ACKer was fixed. If you are using it, upgrade 
    to version 6.0 or later as soon as possible, or use another ACKer implementation.


## Basic configuration

Elastic Actors scans actor and message classes under the packages defined in the file
`classpath:META-INF/elasticactors.properties`. This file is required and contains only the following
property:

```properties
basePackage=your.actor.system.base.package
```

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


## Configuration properties

These properties can be set per-environment using any property source accepted by Spring.
This list is not exhaustive. Specific sections of this README may contain keys specific to them.

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

# The period in milliseconds between cache expiration checks for node-bound actors.
# Expirable actors are checked for expiration when they receive messages, as well as by a
# scheduled executor thread that runs every N milliseconds and removed expired actors.
# This key allows you to adjust how often this thread will run. There is no maximum value, but it 
# enforces a minimum of 500ms so it can't be made to run so frequently.
# Set it to 0 or lower to completely disable the periodic timeout checks.
# Expired actors will still be invalidated when being fetched from the cache.
# Default: 30000
# Minimum: 500
ea.nodeCache.expirationCheckPeriod=30000

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
# Options:
#   - DIRECT: ACKs the messages on the handler thread
#   - BUFFERED: ACKs the messages on one of the handler threads, buffering them in case of high load
#       - **NOTE**: this implementation was broken in older versions of Elastic Actors. 
#                   It would wrongly ACK messages. 
#                   This was fixed in version 6.0.0.
#   - WRITE_BEHIND: ACKs the messages asynchronously using a disruptor
#   - ASYNC: ACKs the messages asynchronously using a separate thread
# Default: DIRECT
ea.rabbitmq.ack=ASYNC

# RabbitMQ thread model.
# Options: 
#   - sc (single-channel): uses producer channel
#   - cpt (channel-per-thread): uses one producer channel for each queue thread
# Default: sc
ea.rabbitmq.threadmodel=cpt

# RabbitMQ prefetch count.
# Default: 0
ea.rabbitmq.prefetchCount=100


## Persistent Actor Repository layer

# Minimum size (in bytes) for which a serialized actor state will be compressed with LZ4
# Default: 512 bytes
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
# Only backplane-cassandra2.
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


## Indexing

# Elasticsearch hosts for indexing Actors
# Required, if using indexing
ea.indexing.elasticsearch.hosts=host1,host2

# Elasticsearch port
# Default: 9300
ea.indexing.elasticsearch.port=9300

# Elasticsearch cluster name
# Default: elasticsearch
ea.indexing.elasticsearch.cluster.name=elasticsearch


## Metrics

# Toggles metrics for messages in shard queues.
# Default: false
ea.metrics.shard.messaging.enabled=false

# Toggles metrics for undeliverable messages in shard queues. 
# Default: false
ea.metrics.shard.messaging.undeliverable.enabled=false

# Toggles metrics for messages using the reactive streams protocol (such as Subscriptions) in shard queues. 
# Default: false
ea.metrics.shard.messaging.reactive.enabled=false

# Configures the threshold for message delivery (total time spent in broker + local queues),
# in microseconds.
# Default: 0
ea.metrics.shard.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds. 
# Default: 0
ea.metrics.shard.messaging.handling.warn.threshold=0

# Configures the threshold for actor state serialization. 
# Default: 0
ea.metrics.shard.serialization.warn.threshold=0

# Toggles metrics for messages in node queues.
# Default: false
ea.metrics.node.messaging.enabled=false

# Toggles metrics for undeliverable messages in node queues. 
# Default: false
ea.metrics.node.messaging.undeliverable.enabled=false

# Toggles metrics for messages using the reactive streams protocol (such as Subscriptions) in node queues. 
# Default: false
ea.metrics.node.messaging.reactive.enabled=false

# Configures the threshold for message delivery (total time spent in broker + local queues),
# in microseconds.
# Default: 0
ea.metrics.node.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds.
# Default: 0
ea.metrics.node.messaging.handling.warn.threshold=0


## Logging

# Toggles logging for messages in shard queues. 
# Default: false
ea.logging.shard.messaging.enabled=false

# Toggles logging for undeliverable messages in shard queues. 
# Default: false
ea.logging.shard.messaging.undeliverable.enabled=false

# Toggles logging for messages using the reactive streams protocol (such as Subscriptions) in shard queues. 
# Default: false
ea.logging.shard.messaging.reactive.enabled=false

# Toggles logging for messages in node queues. 
# Default: false
ea.logging.node.messaging.enabled=false

# Toggles logging for undeliverable messages in node queues. 
# Default: false
ea.logging.node.messaging.undeliverable.enabled=false

# Toggles logging for messages using the reactive streams protocol (such as Subscriptions) in node queues. 
# Default: false
ea.logging.node.messaging.reactive.enabled=false

# Default set of features for logging message types.
# Features defined here have the lowest precedence, meaning any features defined in the 
# @Message annotation or overrides take precedence over this.
# This does not affect internal messages defined in the Elastic Actors framework.
# The value is a list of comma-separated features.
# Refer to the @Message.LogFeature enum for options and their meanings.
# Default: empty
ea.logging.messages.default=BASIC,TIMING,CONTENTS

# Optional LogFeature overrides for specific message types.
# Features defined here have the highest precedence, meaning any features defined here will 
# override both the default and those defined in the @Message annotation for any message type.
# These overrides affect internal messages defined in the Elastic Actors framework too, in case
# that's needed for debugging.
# The value is a list of comma-separated features.
# Refer to the @Message.LogFeature enum for options and their meanings.
# To disable all features, add the key but leave its value empty.
ea.logging.messages.overrides.[class_name]=BASIC,TIMING,CONTENTS

# The maximum number of characters when logging message contents. 
# Default: 5000
ea.logging.messages.maxLength=5000

# If set to true, transient messages will use the toString() method 
# instead of being serialized for logging purposes. 
# Default: false
ea.logging.messages.transient.useToString=false
```


## System properties

The following properties must be provided as system properties, if changing them is desired.

```properties
# Toggles serialization caching. 
# Default: false
ea.serializationCache.enabled=false

# Toggles deserialization caching. 
# Default: false
ea.deserializationCache.enabled=false

# Changes the logging level used when logging unhandled message types in MethodActor.
# Default: WARN
ea.logging.messages.unhandled.level=WARN

# Changes the minimum timeout, in milliseconds, for Temporary Actors.
# It will be clamped to the closest positive integer, if the provided number is negative or 0.
# Default: 1000 (1 second)
ea.tempActor.timeout.min=1000

# Changes the default timeout, in milliseconds, for Temporary Actors.
# It will be clamped so it sits between the minimum and maximum configured numbers.
# Default: 86400000 (one 24h day)
ea.tempActor.timeout.default=86400000

# Changes the maximum timeout, in milliseconds, for Temporary Actors.
# It will be champed to be larger than or equal to the minimum configured number.
# Default: 172800000 (two 24h days)
ea.tempActor.timeout.max=172800000
```


## Class loading cache

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


## Tracing

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
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-tracing</artifactId>
    <scope>runtime</scope>
</dependency>
```

`MessagingContextManager` is the main class responsible for managing trace context data.\
It allows you to put a set of trace and creation contexts into scope.


### Instrumenting Spring beans

In order to instrument asynchronous executor beans automatically, add the following dependency 
to your project:

```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-tracing-spring</artifactId>
    <scope>runtime</scope>
</dependency>
```

This includes a bean post-processor that wraps those facilities in ones
that can automatically propagate the traces from Elastic Actors.


### Adding trace information to logs

The `MessagingContextManager` also has the facilities to add the trace and creation contexts, 
as well as some message handling information, to the logging library's MDC 
(Mapped Diagnostic Context) through Slf4j. This is optional and can be enabled by adding the 
following dependency in your project:

```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-tracing-slf4j</artifactId>
    <scope>runtime</scope>
</dependency>
```

### Metrics

Starting from version 5.2, Elastic Actors supports exporting its internal metrics using [Micrometer](https://micrometer.io/).

All metrics are optional and can be enabled/disabled independently using configuration properties.
If metrics are enabled, Elastic Actors will expect the existence of a bean of type `MeterRegistry` 
and name `elasticActorsMeterRegistry`. The supplied bean will be used to add the metrics.

```properties
# Optional prefix for component names
ea.metrics.micrometer.namePrefix=prefix

# Option global metrics prefix
ea.metrics.micrometer.prefix=elastic.actors

# Toggles Micrometer ON or OFF for a given component.
#
# The following components are currently supported:
# - Caches:
#   - actorRefCache: ActorRef cache
#   - nodeActorCache: Node actor state cache
#   - shardActorCache: Shard actor state cache
# - Thread-bound executors:
#   - queueExecutor: Message queue executor
#   - actorExecutor: Actor message handler executor
#   - asyncUpdateExecutor: Actor state persistence executor
#   - actorStateUpdateProcessor: Actor state update processor executor
# - Messaging services:
#   - rabbitmq: RabbitMQ metrics
#   - rabbitmqAcker: ASYNC ACKer executor service
# - Message schedulers:
#   - scheduler: Scheduler executor service
#
# Default (for all components): false
ea.metrics.micrometer.[component_name].enabled=false

# Optional metric prefix for a given component.
# Component-specific prefixes are inserted after the global prefix.
ea.metrics.micrometer.[component_name].prefix=ea

# Optional custom tags for a given component.
# Keep in mind that all meters created by Elastic Actors will contain at least the following tags:
#   - elastic.actors.generated: true
#   - elastic.actors.node.id: [the node's ID]
#   - elastic.actors.cluster.name: [the cluster's name]
# Can be repeated multiple times for a given component in order to add multiple tags.
ea.metrics.micrometer.[component_name].tags.[tag_name]=tag_value

# Toggles exporting the message delivery times for a given component.
# These are a rough estimate of the time it took from the message being created until it 
# arrived at the receiving actor, in milliseconds.
# Currently, this only applies to the "actorExecutor" component.
# Default: false
ea.metrics.micrometer.[component_name].measureDeliveryTimes=false

# Toggles adding the message wrapper types as tags for a given component.
# Adds the current message's wrapper as the tag "elastic.actors.message.wrapper" and creates a new timer with the wrapper's name as a suffix.
# Currently, this only applies to the "actorExecutor" component.
# Default: false
ea.metrics.micrometer.[component_name].tagMessageWrapperTypes=false

# Toggles adding the task types as tags for a given component.
# Adds the current thread-bound task type as the tag "elastic.actors.message.task" and creates a new timer with the task's name as a suffix.
# Currently, this only applies to the "actorExecutor" component.
# Default: false
ea.metrics.micrometer.[component_name].tagTaskTypes=false

# Allows detailed tagging for the specified actor type for a given component.
# Adds the receiver type as the tag "elastic.actors.actor.type" and creates a new timer with the provided suffix.
# Currently, this only applies to the "actorExecutor" component.
# This might cause some additional overhead, so use this option with caution.
ea.metrics.micrometer.[component_name].detailed.actors.[class_name]=metric.suffix

# Allows detailed tagging for the specified message types for a given component.
# This requires the type of the receiver actor to be present in the list of actors allowed for 
# detailed tagging.
# Adds the receiver type as the tag "elastic.actors.message.type" and creates a new timer with the provided suffix.
# Currently, this only applies to the "actorExecutor" component.
# This might cause some additional overhead, so use this option with caution.
ea.metrics.micrometer.[component_name].detailed.messages.[class_name]=metric.suffix
```

Additionally, customization of tags is supported by providing a bean of type `MeterTagCustomizer`
and name `elasticActorsMeterTagCustomizer`.


### Performance impact of tracing with log context on applications using Log4j2

If running a web application while using Log4j2 for logging, using tracing can generate
a big amount of garbage objects due to Log4j2 using a different implementation of the map
that backs the MDC. In order to avoid performance issues, run your application with the following
system properties set to:

```properties
log4j2.is.webapp=false
log4j2.garbagefree.threadContextMap=true
```

### Release process

This project uses the Maven Release Plugin and GitHub Actions to create releases.\
Just run `mvn release:prepare release:perform` to select the version to be released and create a 
VCS tag. 

GitHub Actions will start [the build process](https://github.com/elasticsoftwarefoundation/elasticactors/actions/workflows/maven-publish.yml). 

If successful, the build will be automatically published to [Maven Central](https://repo.maven.apache.org/maven2/org/elasticsoftwarefoundation/elasticactors/).
