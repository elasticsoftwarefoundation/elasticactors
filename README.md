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

### Configuration keys

```properties
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

### Tracing and Log4j2

If running a web application while using Log4j2 for logging, using tracing can generate
a big amount of garbage objects due to Log4j2 using a different implementation of the map
that backs the MDC. In order to avoid performance issues, run your application with the following
system properties set to:

```properties
log4j2.is.webapp=false
log4j2.garbagefree.threadContextMap=true
```


