# Guide: Using Placeholders for Security and Performance

Placeholders allow you to create parameterized AEL expressions. Instead of embedding literal values directly into your AEL string, you use special markers (starting with `?`) that are replaced with actual values at runtime.

This practice is recommended due to performance enhancement. It allows the AEL parser to compile the expression *once* and reuse the result many times with different values, which is much faster than re-parsing the string for every query.

## The Cost of Parsing

When you provide the `AelParser` with a string, it performs several steps:
1.  **Lexing**: Breaks the string into a stream of tokens (e.g., `$.`, `age`, `>`, `100`).
2.  **Parsing**: Builds an Abstract Syntax Tree (AST) representing the logical structure of the expression.
3.  **Compilation**: Traverses the AST to create a template for the final result.

This process has a small but non-zero CPU cost. If you are parsing the same string inside a tight loop (e.g., for every incoming web request), this cost can add up.

## Placeholder Syntax

Placeholders are denoted by a question mark followed by a zero-based index: `?0`, `?1`, `?2`, and so on.

**AEL String with Placeholders:**
```
"$.age > ?0 and $.city == ?1"
```

Here, `?0` is the first placeholder, and `?1` is the second.

## Providing Placeholder Values

To use an expression with placeholders, you must provide the corresponding values when you parse it. This is done using the `ExpressionContext` and `PlaceholderValues` classes.

### Java Usage Example

```java
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.impl.AelParserImpl;
// ... other imports

AelParser parser = new AelParserImpl();

// The AEL string with indexed placeholders
String ael = "$.age > ?0 and $.city == ?1";

// Create a PlaceholderValues object with the values to substitute.
// The order of values must match the placeholder indexes.
PlaceholderValues values = PlaceholderValues.of(30, "New York");

// Create the ExpressionContext
ExpressionContext context = ExpressionContext.of(ael, values);

// Parse the expression
ParsedExpression parsedExpression = parser.parseExpression(context);

// Now you can get the result and use it in a query
Expression filter = Exp.build(parsedExpression.getResult().getExp());

QueryPolicy queryPolicy = new QueryPolicy();
queryPolicy.filterExp = filter;
```

### Type Handling

The library automatically handles different data types for placeholders, including `Integer`, `Long`, `Double`, `String`, and `byte[]`. The type of the value you provide in `PlaceholderValues` will be translated into the final Aerospike Expression.

**Example with different types:**
```
String ael = "$.lastLogin > ?0 and $.name == ?1";

// Provide a Long for the timestamp and a String for the name
PlaceholderValues values = PlaceholderValues.of(1672531200000L, "Alice");

ExpressionContext context = ExpressionContext.of(ael, values);
// ...
```

## Reusing Parsed Expressions

The most significant performance benefit of placeholders comes from reusing the `ParsedExpression` object. The parsing process (translating the string into an internal structure) only needs to happen once. After that, you can efficiently substitute new values.

### High-Performance Example

Imagine an application that needs to query for users by age and city repeatedly.

```java
// --- One-time setup ---
AelParser parser = new AelParserImpl();
String ael = "$.age > ?0 and $.city == ?1";
ExpressionContext initialContext = ExpressionContext.of(ael); // No values needed at first

// Parse the expression once and cache the result
ParsedExpression cachedParsedExpression = parser.parseExpression(initialContext);


// --- In your application's request-handling logic (called many times) ---

public void findUsers(int age, String city) {
    // Create PlaceholderValues for the current request
    PlaceholderValues currentValues = PlaceholderValues.of(age, city);

    // Get the result by substituting new values. This is very fast!
    ParseResult result = cachedParsedExpression.getResult(currentValues);

    Expression filter = Exp.build(result.getExp());
    
    // Execute query with the filter...
}
```

By following this pattern, you minimize parsing overhead and create more efficient applications.