Elastic Actors
=============

Persistent Stateful Actor System

### Current released version

[ ![Download](https://api.bintray.com/packages/elasticsoftwarefoundation/maven/org.elasticsoftware.elasticactors/images/download.svg) ](https://bintray.com/elasticsoftwarefoundation/maven/org.elasticsoftware.elasticactors/_latestVersion)

### Add Elastic Actors to your Project

repository:
```xml
<repositories>
    <repository>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
        <id>elasticsoftwarefoundation</id>
        <name>bintray</name>
        <url>http://dl.bintray.com/elasticsoftwarefoundation/maven</url>
    </repository>
</repositories>
```
minimal dependency:
```xml
<dependency>
    <groupId>org.elasticsoftware.elasticactors</groupId>
    <artifactId>elasticactors-api</artifactId>
    <version>${elasticactors.version}</version>
</dependency>
```
convenient base classes inclusing a Jackson 2 based serialization framework:
```xml
<dependency>
    <groupId>org.elasticsoftware.elasticactors</groupId>
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

ActorSystem serializationRegistry = testActorSystem.getActorSystem();
ActorRef greeter = serializationRegistry.actorOf("greeter",GreetingActor.class);
greeter.tell(new Greeting("Joost van de Wijgerd"),null);

testActorSystem.destroy();
```








