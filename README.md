# Aerospike Expression Language
[![Build project](https://github.com/citrusleaf/aerospike-expression-lang-java/actions/workflows/build.yml/badge.svg)](https://github.com/citrusleaf/aerospike-expression-lang-java/actions/workflows/build.yml)

> [!NOTE]
> This is a developer preview, not yet ready for production use, but with future releases planned. We appreciate feedback from the Aerospike 
> developer community through GitHub issues.

Aerospike Expression Language (AEL) is a Java library that allows translating a canonical AEL string into an Aerospike Expression.

[Aerospike Expressions](https://aerospike.com/docs/server/guide/expressions) is a functional language for applying predicates to bin data and record metadata.  
Expressions are essential to different areas of server functionality:
* Secondary indexes
* Filter Expressions (introduced in Aerospike Database 5.2)
* XDR Filter Expressions (introduced in Aerospike Database 5.3)
* Operation Expressions (introduced in Aerospike Database 5.6)

## Why AEL
A filter expression is created by combining the expression predicate classes using Polish notation, compiling it, and then attaching it to the operation.
This approach has several limitations, including a difficult developer experience.

Expressions require the use of native language clients. As a result, filter expressions cannot be used in places that only accept a filter written in text.
As a workaround, expressions can be created separately using a native client and exported as a base64-encoded string.

A text-based AEL can be converted into Expressions and be used across frameworks, connectors, gateways, data browsers, IDE plug-ins, and all language-native clients.
Having a standard way to describe filters is easy to document and makes it accessible to developers regardless of their language preferences.

## Prerequisites
* Java 17 or later
* Aerospike Server version 5.2+

## Build from source
Aerospike Expression Language uses [ANTLR v4](https://github.com/antlr/antlr4) for its parsing capabilities.  
When modifying the grammar file you will need to re-generate the ANTLR sources by running the following command:

`mvn clean generate-sources compile`

## Usage examples

### Parsing AEL expression

```java
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.impl.AelParserImpl;

// String AEL expression
String input = "$.intBin1 > 100 and $.intBin2 > 100";
// Instantiating AEL parser
AelParser parser = new AelParserImpl();
// Providing list of existing secondary indexes
// At most only one index will be chosen based on its namespace and binValuesRatio (cardinality)
// If cardinality of multiple indexes is the same, the index is chosen alphabetically
// binValuesRatio must be non-negative; defaults to 0 if omitted
List<Index> indexes = List.of(
        Index.builder().namespace("namespace").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
        Index.builder().namespace("namespace2").bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
);
// Parsing AEL expression
ParsedExpression expression = parser.parseExpression(
        // We provide expression context (it may also contain placeholders values as an optional argument)
        ExpressionContext.of(input), 
        // And optional index context with the required namespace and a list of existing indexes
        IndexContext.of("namespace", indexes)
);
```

### Getting filter expression

```java
Expression actualExp = Exp.build(
        // Getting filter expression
        expression.getResult().getExp()
);

// Only one filter expression because the first part has secondary index filter
Expression expectedExp = Exp.build(
    Exp.gt(
        Exp.intBin("intBin2"),
        Exp.val(100))
);

assertEquals(actualExp, expectedExp);
```

### Checking bin or element existence

```java
// Check if a bin exists
Exp exp1 = parser.parseExpression(ExpressionContext.of("$.myBin.exists()")).getResult().getExp();

// Check if a map key exists, combined with another condition
Exp exp2 = parser.parseExpression(
        ExpressionContext.of("$.config.featureFlag.exists() and $.status == 1")
).getResult().getExp();
```

### Getting secondary index filter

```java
// Getting secondary index filter
// Only one secondary index filter can be created at most
Filter actualFilter = expression.getResult().getFilter();

// The second part has filter expression
Filter expectedFilter = Filter.range("intBin1", 101, Long.MAX_VALUE);

assertEquals(actualFilter, expectedFilter);
```

## License
Licensed under the Apache 2.0 License.
