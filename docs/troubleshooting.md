# Troubleshooting & FAQ

This guide provides solutions to common problems and answers frequently asked questions about the Aerospike Expression Language.

## Common Errors and Solutions

### `AelParseException`

This is the most common exception thrown by the library, indicating a problem with the AEL string itself.

| Cause                       | Example | Solution                                                                                                                                    |
|:----------------------------| :--- |:--------------------------------------------------------------------------------------------------------------------------------------------|
| **Mismatched Parentheses**  | `($.age > 30` | Ensure every opening parenthesis `(` has a corresponding closing parenthesis `)`.                                                           |
| **Invalid Operator**        | `$.age ** 30` | Check the spelling of all operators. Valid operators include `and`, `or`, `not`, `==`, `!=`, `>`, `>=`, `<`, `<=`, `+`, `-`, `*`, `/`, `%`. |
| **Unquoted String Literal** | `$.city == New York` | Enclose all string literals in single quotes: `$.city == 'New York'`.                                                                       |
| **Incomplete Expression**   | `$.age >` | Ensure every operator has the correct number of operands.                                                                                   |
| **Incompatible Types**      | `($.apples.get(type: STRING) + 5) > 10` | Cannot compare STRING to INT                                                                                                                |

### `IllegalArgumentException: Missing value for placeholder`

This error occurs when your AEL string contains placeholders (`?0`, `?1`, etc.), but you did not provide a corresponding value in the `PlaceholderValues` object.

**Example AEL:** `$.age > ?0 and $.city == ?1`

**Incorrect Code:**
```java
// Only one value provided, but two are needed
ExpressionContext.of(ael, PlaceholderValues.of(30)); 
```

**Solution:** Ensure you provide a value for every placeholder in the correct order.

```java
ExpressionContext.of(ael, PlaceholderValues.of(30, "New York"));
```

### Query is Slow or Scans the Entire Dataset

If your filtered query is not performing as expected, it may be because it is not leveraging a secondary index.

*   **Symptom**: A query on a highly selective field (e.g., a unique user ID) takes a long time.
*   **Cause**: The query is using a filter expression to scan all records on the server instead of using a secondary index to jump directly to the candidate records.
*   **Solution**:
    1.  Ensure a secondary index exists on the filter predicate.
    2.  Provide an `IndexContext` to the `parseExpression` method. This enables the parser to perform automatic query optimization. See the **"Leveraging Secondary Indexes Automatically"** guide for a detailed walkthrough.