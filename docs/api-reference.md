# API Reference

The Expression Language library has a concise API. This document highlights the most important classes and methods you will interact with.

For a complete, detailed reference, please consult the Javadoc for the library.

## Core Classes

### `com.aerospike.ael.api.AelParser`

This is the main interface and entry point for the library.

*   **`ParsedExpression parseExpression(ExpressionContext input)`**
    *   **Description**: The primary method used to parse a AEL string when no secondary indexes are provided. It returns a `ParsedExpression` object that contains the compiled result, which can be reused.
*   **`ParsedExpression parseExpression(ExpressionContext input, IndexContext indexContext)`**
    *   **Description**: The primary method used to parse a AEL string. It returns a `ParsedExpression` object that contains the compiled result, which can be reused.
    *   **Parameters**:
        *   `input`: An `ExpressionContext` object containing the AEL string and any placeholder values.
    *   **Description**: The primary method used to parse a AEL string. It returns a `ParsedExpression` object that contains the compiled result, which can be reused.
    *   **Parameters**:
        *   `input`: An `ExpressionContext` object containing the AEL string and any placeholder values.
        *   `indexContext`: An optional `IndexContext` object containing a list of available secondary indexes for query optimization. Can be `null`.
    *   **Returns**: A `ParsedExpression` object representing the compiled expression tree.

### `com.aerospike.ael.ExpressionContext`

This class is a container for the AEL string and any values to be substituted for placeholders.

*   **`static ExpressionContext of(String aelString)`**: Creates a context for a AEL string without placeholders.
*   **`static ExpressionContext of(String aelString, PlaceholderValues values)`**: Creates a context for a AEL string that uses `?` placeholders, providing the values to be substituted.

### `com.aerospike.ael.ParsedExpression`

This object represents the compiled, reusable result of a parsing operation. It is thread-safe.

*   **`ParseResult getResult()`**: Returns the final `ParseResult` for an expression that does not contain placeholders.
*   **`ParseResult getResult(PlaceholderValues values)`**: Returns the final `ParseResult` by substituting the given placeholder values into the compiled expression tree. This is highly efficient as it bypasses the parsing step.

### `com.aerospike.ael.ParseResult`

This class holds the final, concrete outputs of the parsing and substitution process.

*   **`Filter getFilter()`**: Returns an Aerospike `Filter` object if the parser was able to optimize a portion of the AEL string into a secondary index query. Returns `null` if no optimization was possible.
*   **`com.aerospike.client.exp.Expression.Exp getExp()`**: Returns the Aerospike `Exp` object representing the AEL filter logic. This is the part of the expression that will be executed on the server for records that pass the secondary index filter. If the entire AEL string was converted into a `Filter`, this may be `null`.

### `com.aerospike.ael.IndexContext`

A container for the information required for automatic secondary index optimization.

*   **`static IndexContext of(String namespace, Collection<Index> indexes)`**: Creates a context.
    *   `namespace`: The namespace the query will be run against. Must not be null or blank.
    *   `indexes`: A collection of `Index` objects representing the available secondary indexes for that namespace.
*   **`static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse)`**:
    Creates a context with an explicit index name hint.
    *   `namespace`: The namespace the query will be run against. Must not be null or blank.
    *   `indexes`: A collection of `Index` objects representing the available secondary indexes for that namespace.
    *   `indexToUse`: The name of the index to use for the secondary index filter.
    If not found, `null`, or empty, the index is chosen automatically by cardinality
    (higher `binValuesRatio` preferred), then alphabetically by bin name.
*   **`static IndexContext withBinHint(String namespace, Collection<Index> indexes, String binToUse)`**:
    Creates a context with an explicit bin name hint. Use this when you want to direct the parser to
    an index on a specific bin without knowing the index name.
    *   `namespace`: The namespace the query will be run against. Must not be null or blank.
    *   `indexes`: A collection of `Index` objects representing the available secondary indexes for that namespace.
    *   `binToUse`: The name of the bin whose index should be preferred. If one or more indexes in the collection
    match the given bin name and namespace, the parser prefers that bin and selects a specific index among those
    matches using the normal automatic rules (for example, index type and cardinality, then alphabetically).
    If no index matches the bin and namespace, or `binToUse` is `null` or blank, the parser falls back to fully
    automatic index selection.
### `com.aerospike.ael.Index`

Represents an available secondary index for optimization.

*   **Mandatory fields**: `namespace`, `bin`, `indexType`.
*   **Optional field**: `binValuesRatio` — an estimate of the cardinality of values in the indexed bin.
*   **`binValuesRatio` behavior and validation**:
    * If omitted, it defaults to `0`.
    * Must be non-negative (`>= 0`).
    * Providing a realistic, non-negative cardinality value is recommended for better automatic index selection (indexes with higher `binValuesRatio` are preferred).

## CDT path segments and ranges

Expressions may use **closed-boundaries** and **half-open CDT ranges** on maps and lists. Details in the 
[Guide: Working with Lists and Maps — CDT ranges](guides/02-working-with-lists-and-maps.md#cdt-ranges).

The **`parseCTX`** API accepts the same surface syntax as paths embedded in expressions, 
but **only simple selectors** produce `CTX[]` (single key, index, value, rank, etc.). 
**Range segments** (index/value/key intervals) are not converted to context entries and will fail with an error 
indicating that context is not supported for that part type.

## Path Functions

Path functions are suffixed to a bin or CDT path with dot notation.

| Function | Description | Example |
|----------|-------------|---------|
| `get()` | Retrieve a value (default if omitted). Accepts optional `type` and `return` params. | `$.mapBin.key.get(type: INT)` |
| `count()` | Count elements in a CDT bin or at a CDT path. | `$.listBin.count() > 5` |
| `exists()` | Return `true` if a bin or CDT element exists, `false` otherwise. | `$.myBin.exists()` |
| `asInt()` / `asFloat()` | Cast a CDT element value to int or float. | `$.listBin.[0].asInt()` |

### `exists()` Details

**Bin-level:** `$.binName.exists()` returns a boolean predicate checking whether the bin is present in the record.

**CDT-level:** `$.mapBin.key.exists()` or `$.listBin.[0].exists()` checks whether the specified element exists within the CDT structure.

`exists()` can be used standalone, in logical expressions (`and`, `or`, `not`), or as a condition in `when` expressions.

## Example API Flow

Here is a recap of how the classes work together in a typical use case:

```java
// 1. Get a parser instance
AelParser parser = new AelParserImpl();

// 2. Define the context for the expression and placeholders
String ael = "$.age > ?0";
ExpressionContext context = ExpressionContext.of(ael, PlaceholderValues.of(30));

// (Optional) Define the index context for optimization
IndexContext indexContext = IndexContext.of("namespace", availableIndexes);

// 3. Parse the expression once to get a reusable object
ParsedExpression parsedExpression = parser.parseExpression(context, indexContext);

// 4. Get the final result by substituting values
// This step can be repeated many times with different values
ParseResult result = parsedExpression.getResult(); 

// 5. Extract the Filter and Expression for use in a QueryPolicy
Filter siFilter = result.getFilter();
Expression filterExp = Exp.build(result.getExp());

QueryPolicy policy = new QueryPolicy();
policy.filterExp = filterExp;
// Note: The Java client does not have a separate field for the secondary index filter.
// The filter is applied by the client before sending the query.
```