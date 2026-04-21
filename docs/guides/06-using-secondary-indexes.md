# Guide: Automatic Secondary Index Optimization

One of the most powerful features of the Expression Language is its ability to automatically leverage accessible Aerospike secondary indexes (SI).

When you provide the parser with a list of available indexes, it can analyze your AEL string and transform a part of it into a more performant secondary index filter.

## Why is this important?

*   **Performance**: A secondary index query is significantly faster than a full scan with a filter expression. An SI query allows the database to jump directly to the records that might match, whereas a filter expression requires the server to read every single record in the set and evaluate the expression against it.
*   **Simplicity**: You don't need to manually decide which parts of your query should use an index. You can write a single, logical AEL string, and the parser will perform the optimization for you given that you 

## How it Works

When you call `parser.parseExpression()`, you can optionally provide an `IndexContext`. This object tells the parser which namespace you are querying and which secondary indexes are available.

The parser then does the following:
1.  It analyzes the `and` components of your AEL expression if there are any.
2.  It compares each component against the list of available indexes.
3.  If it finds a component that can be satisfied by a numeric or string range query on an indexed bin, it converts that component into an Aerospike secondary index `Filter` object.
4.  It marks that component in order not to use it when building `Expression`.
5.  The rest of the AEL string is converted into an `Expression`.
6.  The `ParseResult` then contains either one of the following entities or **both**:
* `Filter` (for the SI query) - given that the correct `IndexContext` was provided, and that the given AEL expression can be converted to a secondary index filter
* `Expression` (for the scan expression filter)

> **Note:** Only one secondary index can be used per query. The index will be chosen based on cardinality
> (preferring indexes with a higher `binValuesRatio`), otherwise alphabetically.
> Type-matching is also applied when multiple indexes exist on the same bin.

## Choosing a Specific Index (Index Hint)

When multiple indexes could satisfy your query, the parser selects one automatically.
You can override this by providing an explicit hint in two ways:

### Hint by Index Name

If you know the exact name of the index you want to use, pass it as the third argument to
the three-parameter `IndexContext.of` overload:

```java
Index cityIndex = Index.builder()
    .namespace("test") // is mandatory
    .bin("city") // is mandatory
    .indexType(IndexType.STRING) // is mandatory
    .binValuesRatio(1) // defaults to 0; must be non-negative
    .name("idx_users_city")
    .build();

Index ageIndex = Index.builder()
    .namespace("test")
    .bin("age")
    .indexType(IndexType.NUMERIC)
    .binValuesRatio(10)
    .name("idx_users_age")
    .build();

// Force use of the city index even though age has higher cardinality
IndexContext indexContext = IndexContext.of("test", List.of(cityIndex, ageIndex), "idx_users_city");
```

If the named index is not found in the collection or does not match the namespace,
the parser falls back to automatic selection (cardinality, then alphabetically).
Passing `null` or a blank string as the third parameter also triggers automatic selection.

If the named index **is** found but cannot be applied to the expression (for example, the index
is on a different bin than any bin referenced in the query, or has a type mismatch),
the parser falls back to automatic selection across all provided indexes.
If no suitable index exists in the collection, no secondary index filter is produced.

### Hint by Bin Name

If you know which bin should be used for the secondary index filter but not the specific index name,
use `IndexContext.withBinHint`:

```java
Index cityIndex = Index.builder()
    .namespace("test")
    .bin("city")
    .indexType(IndexType.STRING)
    .binValuesRatio(1)
    .name("idx_users_city")
    .build();

Index ageIndex = Index.builder()
    .namespace("test")
    .bin("age")
    .indexType(IndexType.NUMERIC)
    .binValuesRatio(10)
    .name("idx_users_age")
    .build();

// Force use of an index on the "city" bin even though age has higher cardinality
IndexContext indexContext = IndexContext.withBinHint("test", List.of(cityIndex, ageIndex), "city");
```

If the hinted bin name matches no indexes in the collection, the hint is ignored and the parser falls back
to fully automatic selection (by cardinality, then alphabetically) across all given indexes.
Passing `null` or a blank string bin name hint also triggers fallback to automatic selection.

If one or more provided indexes match the hinted bin name and namespace, the parser prefers an index on that bin. 
When multiple indexes exist on the same bin (e.g., a STRING index and a NUMERIC index), 
automatic type-based and cardinality-based selection takes place — 
for example, in a numeric expression like `$.value == 19` the NUMERIC index on `value` will be picked.

If the preferred bin's indexes cannot be applied to the expression (e.g. there is a type mismatch), 
the parser falls back to automatic selection across all provided indexes. 
If no suitable index exists, no secondary index filter is produced.


## Usage Example

Let's assume you have a secondary index named `idx_users_city` on the `city` bin in the `users` set.

### 1. Define the Index Information

First, you need to represent your available index in code.

```java
import com.aerospike.client.query.IndexType;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import java.util.List;

// Describe the available secondary index
// namespace, bin, indexType are required, binValuesRatio must be non-negative
Index cityIndex = Index.builder()
    .namespace("test")
    .bin("city")
    .indexType(IndexType.STRING)
    .binValuesRatio(1) // Cardinality from Aerospike sindex-stat or set manually
    .name("idx_users_city")
    .build();

// Create index context (namespace must not be null or blank)
IndexContext indexContext = IndexContext.of("test", List.of(cityIndex));
```

### 2. Parse the Expression using IndexContext

Now, provide the `indexContext` when you parse your AEL string.

**AEL String:**
```
"$.city == 'New York' and $.age > 30"
```

**Java Code:**
```java
AelParser parser = new AelParserImpl();
String ael = "$.city == 'New York' and $.age > 30";
ExpressionContext context = ExpressionContext.of(ael);

// Provide the IndexContext to enable using SI filter
ParsedExpression parsed = parser.parseExpression(context, indexContext);
ParseResult result = parsed.getResult();
```

### 3. Extract Both Filter and Expression
```java

// 3. Extract Both Filter and Expression
Filter siFilter = result.getFilter(); // This will be non-null if indexes are correct and AEL string input allows building SI filter, like in this example
Expression filterExpression = Exp.build(result.getExp()); // This will contain the scan expression filter

// The parser has split the query:
// siFilter is now a Filter.equal("city", "New York")
// filterExpression is now Exp.gt(Exp.intBin("age"), Exp.val(30))
```
### 4. Execute the Query

When you execute the query, you need to use both the `Filter` and the `Expression`. The Java client handles this
by applying the secondary index `Filter` first to select the initial set of records, 
and then applying the `filterExp` on the server to those results.

```java
Statement stmt = new Statement();
stmt.setNamespace("test");
stmt.setSetName("users");

// Apply the secondary index filter
stmt.setFilter(siFilter);

// Apply the remaining filter expression
QueryPolicy queryPolicy = new QueryPolicy();
queryPolicy.filterExp = filterExpression;

// Execute the highly optimized query
client.query(queryPolicy, stmt);
```

By providing the `IndexContext`, you have allowed the AEL parser to transform a potentially slow scan into a fast, targeted query, without changing your original AEL logic.