Elastic Actors
=============

Persistent Stateful Actor System

### Current released version

![CI](https://github.com/elasticsoftwarefoundation/elasticactors/workflows/CI/badge.svg)
[![License: Apache 2](https://img.shields.io/badge/LICENSE-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![Maven Central](https://img.shields.io/maven-central/v/org.elasticsoftwarefoundation/elasticactors.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.elasticsoftwarefoundation%22%20AND%20a:%22elasticactors%22)

### Add Elastic Actors to your Project

minimal dependency:
```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-api</artifactId>
    <version>${elasticactors.version}</version>
</dependency>
```
convenient base classes inclusing a Jackson 2 based serialization framework:
```xml
<dependency>
    <groupId>org.elasticsoftwarefoundation.elasticactors</groupId>
    <artifactId>elasticactors-base</artifactId>
    <version>${elasticactors.version}</version>
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








