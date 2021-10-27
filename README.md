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
# Toggles logging for messages in node queues
ea.logging.node.messaging.enabled=false

# Toggles metrics for messages in node queues
ea.metrics.node.messaging.enabled=false

# Configures the threshold for message delivery (total time spent in broker + local queues)
# in microseconds
ea.metrics.node.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds
ea.metrics.node.messaging.handling.warn.threshold=0

# Toggles logging for messages in shard queues
ea.logging.shard.messaging.enabled=false

# Toggles metrics for messages in shard queues
ea.metrics.shard.messaging.enabled=false

# Configures the threshold for message delivery (total time spent in broker + local queues)
# in microseconds
ea.metrics.shard.messaging.delivery.warn.threshold=0

# Configures the threshold for message handling in microseconds
ea.metrics.shard.messaging.handling.warn.threshold=0

# Configures the threshold for actor state serialization
ea.metrics.shard.serialization.warn.threshold=0

# Configures LogFeature overrides for specific message types
# The value is a list of comma-separated features
ea.metrics.messages.overrides.{{class_name}}=TIMING,CONTENTS

ea.logging.message.maxLength=10
ea.logging.message.transient.useToString=true
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





