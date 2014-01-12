Elastic Actors
=============

Persistent Stateful Actor System

### Current released version

0.2.1

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
    <version>${elasticactors.version}/version>
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








