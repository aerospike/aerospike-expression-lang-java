# Getting Started with Aerospike Expression Language

Welcome to the Aerospike Expression Language for Java! Let's walk you through the first steps, from setup to running your first query.

## What is the Expression Language?

The Aerospike Expression Language is a Java library that provides a simple, string-based language to create powerful server-side filters. Instead of building complex filter objects in Java, you can write an intuitive expression string, which the library translates into a native Aerospike Expression.

For example, instead of writing this in Java:

```java
Expression exp = Exp.build(
    Exp.and(
        Exp.gt(Exp.intBin("age"), Exp.val(30)),
        Exp.eq(Exp.stringBin("country"), Exp.val("US"))
    )
);
```

You can simply write this string:

```
"$.age > 30 and $.country == 'US'"
```

This makes your filter logic easier to write, read, and even store as configuration.

## Quickstart: Your First Filtered Query

Let's build and run a complete example.

### Prerequisites

1.  **Java 17+**: Ensure you have a compatible JDK installed.
2.  **Maven or Gradle**: For managing dependencies.
3.  **Aerospike Database**: An Aerospike server instance must be running. The easiest way to get one is with Docker:
    ```sh
    docker run -d --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 aerospike/aerospike-server-enterprise
    ```

### 1. Project Setup

Add the Expression Language and the Aerospike Java Client as dependencies to your project.

**Maven (`pom.xml`):**
```xml
<dependencies>
    <dependency>
        <groupId>com.aerospike</groupId>
        <artifactId>aerospike-expression-lang-java</artifactId>
        <version>0.1.0</version> <!-- Check for the latest version -->
    </dependency>
    <dependency>
        <groupId>com.aerospike</groupId>
        <artifactId>aerospike-client-jdk8</artifactId>
        <version>8.1.1</version> <!-- Check for a compatible version -->
    </dependency>
</dependencies>
```

### 2. Write the Code

Here is a Java example. It connects to a local Aerospike instance, writes a few sample records, and then uses a AEL expression to query for a subset of that data.

```java
import com.aerospike.client.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.impl.AelParserImpl;

public class AelQuickstart {

    public static void main(String[] args) {
        // 1. Connect to Aerospike
        try (AerospikeClient client = new AerospikeClient("127.0.0.1", 3000)) {
            // 2. Write some sample data
            writeSampleData(client);

            // 3. Define and Parse the AEL Expression
            AelParser parser = new AelParserImpl();
            String aelString = "$.age > 30 and $.city == 'New York'";
            System.out.println("Using AEL Expression: " + aelString);

            ParsedExpression parsedExpression = parser.parseExpression(ExpressionContext.of(aelString));
            Expression filterExpression = Exp.build(parsedExpression.getResult().getExp());

            // 4. Create and Execute the Query
            QueryPolicy queryPolicy = new QueryPolicy();
            queryPolicy.filterExp = filterExpression;

            Statement stmt = new Statement();
            stmt.setNamespace("test");
            stmt.setSetName("users");

            System.out.println("\nQuery Results:");
            try (RecordSet rs = client.query(queryPolicy, stmt)) {
                while (rs.next()) {
                    System.out.println(rs.getRecord());
                }
            }
        }
    }

    private static void writeSampleData(AerospikeClient client) {
        String namespace = "test";
        String setName = "users";

        client.put(null, new Key(namespace, setName, "user1"),
            new Bin("name", "Alice"), new Bin("age", 28), new Bin("city", "San Francisco"));
        client.put(null, new Key(namespace, setName, "user2"),
            new Bin("name", "Bob"), new Bin("age", 35), new Bin("city", "New York"));
        client.put(null, new Key(namespace, setName, "user3"),
            new Bin("name", "Charlie"), new Bin("age", 42), new Bin("city", "New York"));
        client.put(null, new Key(namespace, setName, "user4"),
            new Bin("name", "Diana"), new Bin("age", 29), new Bin("city", "Chicago"));
            
        System.out.println("Sample data written.");
    }
}
```

### 3. Run and Verify

When you run this code, you will see the following output. Notice that only the two records matching the AEL filter (`age > 30` AND `city == 'New York'`) are returned.

```
Sample data written.
Using AEL Expression: $.age > 30 and $.city == 'New York'

Query Results:
(gen:1),(exp:486523),(bins:(name:Bob),(age:35),(city:New York))
(gen:1),(exp:486523),(bins:(name:Charlie),(age:42),(city:New York))
```

Congratulations! You've successfully used the Expression Language to filter records in Aerospike.

### Next Steps

*   Explore the **Core Concepts in How-To Guides** to learn about more advanced filtering capabilities.
*   Check out the **Installation & Setup Guide** for detailed configuration options.
