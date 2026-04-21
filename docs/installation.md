# Installation & Setup

This guide provides detailed instructions for adding the Aerospike Expression Language library to your project and configuring it.

## Prerequisites

*   **Java 17 or later**
*   See the minimal supported version of **Aerospike Server** in the compatibility table below
*   A build tool such as **Maven** or **Gradle**.

## Library Installation

To use the Expression Language, you need to add this dependency to your project: `aerospike-expression-lang-java`. Library versions prior to 0.3.0 had an additional dependency `aerospike-client-jdk8`.

### Maven

Add the following dependencies to your `pom.xml` file:

```xml
<dependencies>
    <!-- The Expression Language Library -->
    <dependency>
        <groupId>com.aerospike</groupId>
        <artifactId>aerospike-expression-lang-java</artifactId>
        <version>0.3.0</version>
    </dependency>
</dependencies>
```

### Gradle

Add the following to your `build.gradle` file's `dependencies` block:

```groovy
dependencies {
    // The Expression Language Library
    implementation 'com.aerospike:aerospike-expression-lang-java:0.3.0'
}
```

## Initializing the Parser

The main entry point for the library is the `AelParser` interface. To get started, simply instantiate the default implementation:

```java
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.impl.AelParserImpl;

// Create a reusable parser instance
AelParser parser = new AelParserImpl();
```

This `parser` object is thread-safe and can be reused across your application to parse different AEL expression strings.

## Compatibility Table

It is important to ensure the versions of the AEL library, Java client, and Aerospike Server are compatible.

NOTE: Starting with version `0.3.0`, there is no dependency on Java client.

| `aerospike-expression-lang-java` | `aerospike-client-jdk8` | Aerospike Server |
|:---------------------------|:------------------------|:-----------------|
| 0.1.0                      | 8.0.0+                  | 5.7+             |
| 0.2.0                      | 8.0.0+                  | 5.7+             |
| 0.3.0                      | -                       | 6.1+             |

## Building from Source (Optional)

If you need to build the library from source, you will need to regenerate the ANTLR sources first. 

The grammar file is located at `src/main/antlr4/com/aerospike/ael/Condition.g4`.

Run the following Maven command to re-generate the Java parser classes:

```sh
mvn clean generate-sources compile
```