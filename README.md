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
shard hashing seed may change the shard in which the actor resides and thus it would not 
be found anymore.

```yaml
---
# The name and number of shards in this Actor System
name: test
shards: 8

# Optional keys for performance tuning. 
# Refer to ActorSystemConfiguration for more details.
#queuesPerNode: 8
#queuesPerShard: 2
#shardHashSeed: 0
#multiQueueHashSeed: 53
#shardDistributionHashSeed: 0

# Remote actor systems configuration
# In order for Actor Systems to communicate with one another, they must know
# how to find each other and how to map actor IDs to shards and queues on 
# the other system
remoteActorSystems:
  - clusterName: test2.elasticsoftware.org
    name: default
    shards: 8
    #queuesPerShard: 2
  - clusterName: test3.elasticsoftware.org
    name: default
    shards: 8
    #queuesPerShard: 3
    
# Any other objects will be read as properties for a given actor type. 
# The General format is:
actor.type:
  keys: values
  
# These are accessible through ActorSystemConfiguration. Example:
elasticactors.http.actors.HttpService:
  listenPort: 8080
elasticactors.test.TestActor:
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

```properties
## Basic configuration
ea.node.config.location=file:/etc/config.yaml
ea.cluster=cluster.name

## Jackson serialization framework
ea.base.useAfterburner=true

## Caching and tuning
ea.nodeCache.maximumSize=10240
ea.shardCache.maximumSize=10240
ea.actorRefCache.maximumSize=10240
ea.actorExecutor.workerCount=32
ea.actorExecutor.useDisruptor=false
ea.queueExecutor.workerCount=16
ea.queueExecutor.useDisruptor=false
ea.asyncUpdateExecutor.workerCount=10
ea.asyncUpdateExecutor.batchSize=1
ea.shardedScheduler.useNonBlockingWorker=true

## K8s clustering library
ea.cluster.kubernetes.statefulsetName=actor-system-set
ea.cluster.kubernetes.useDesiredReplicas=false

## RabbitMQ messaging layer
ea.rabbitmq.hosts=127.0.0.1
ea.rabbitmq.username=guest
ea.rabbitmq.password=guest
ea.rabbitmq.ack=ASYNC
ea.rabbitmq.threadmodel=cpt
ea.rabbitmq.prefetchCount=100

## Cassandra backplane
ea.cassandra.hosts=127.0.0.1
ea.cassandra.hfactory.manageCluster=false
ea.persistentActorRepository.compressionThreshold=4096


## Metrics and Logging

# Toggles metrics for messages in node queues
ea.metrics.node.messaging.enabled=false

# Configures the threshold for message delivery (total time spent in broker + local queues)
# in microseconds
ea.metrics.node.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds
ea.metrics.node.messaging.handling.warn.threshold=0

# Configures the threshold for message delivery (total time spent in broker + local queues)
# in microseconds
ea.metrics.shard.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds
ea.metrics.shard.messaging.handling.warn.threshold=0

# Configures the threshold for actor state serialization
ea.metrics.shard.serialization.warn.threshold=0

# Toggles logging for messages in shard queues
ea.logging.shard.messaging.enabled=false

# Toggles logging for messages in node queues
ea.logging.node.messaging.enabled=false

# Configures LogFeature overrides for specific message types
# The value is a list of comma-separated features
ea.logging.messages.overrides.{{class_name}}=TIMING,CONTENTS

# The maximum number of characters when logging message contents 
ea.logging.messages.maxLength=5000

# If set to true, transient messages will use the toString() method 
# instead of being serialized for logging purposes
ea.logging.messages.transient.useToString=false

# Changes the logging level used when logging unhandled message types in MethodActor
# this must be entered as a system property
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


