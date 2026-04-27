# Advanced Topic: Arithmetic Expressions

The Expression Language allows you to perform certain simple arithmetic operations directly on bin values within your expressions. This enables you to push mathematical computations to the Aerospike server, avoiding the need to pull data to the client for processing.

This is useful for a wide range of scenarios, such as dynamic price calculations, scoring, or checking computed thresholds.

## Supported Arithmetic Operators

The AEL supports the standard set of arithmetic operators:

*   `+` (Addition)
*   `-` (Subtraction)
*   `*` (Multiplication)
*   `/` (Division)
*   `%` (Modulo)

You can use these operators on numeric bin values and literal numeric values.

## Use Case: Dynamic Thresholds

Imagine an e-commerce application where you want to find all orders that have a `quantity` of at least 5 and for which the `order_total` is greater than `quantity * price_per_item * 0.9` (representing a 10% discount threshold).

### AEL String

You can write this complex logic as a single, clear expression:

```
"$.quantity >= 5 and $.order_total > ($.quantity * $.price_per_item * 0.9)"
```

### How it Works

For each record being scanned, the server will:
1.  Evaluate the first condition: `$.quantity >= 5`. If `false`, the record is skipped.
2.  If `true`, it evaluates the arithmetic part:
    a. It reads the value from the `quantity` bin.
    b. It reads the value from the `price_per_item` bin.
    c. It multiplies them together, and then multiplies by `0.9`.
3.  It then compares the `order_total` bin's value with the computed result.
4.  If the condition is met, the record is returned.

This entire computation happens on the server, which is efficient.

**Java Usage:**
```java
// Find orders with quantity larger than 5 and arithmetical condition on order_total
String ael = "$.quantity >= 5 and $.order_total > ($.quantity * $.price_per_item * 0.9)";
ExpressionContext context = ExpressionContext.of(ael);

ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());

// Setting the resulting Expression as query filter
QueryPolicy queryPolicy = new QueryPolicy();
queryPolicy.filterExp = filter;
// ... execute query
```

## Combining with Placeholders

Arithmetic expressions can be combined with placeholders to make them even more flexible.

### Use Case: Finding recent high-value activity

Let's say you want to find users whose `login_streak` (number of consecutive days logged in) is greater than their `account_age` (in days) divided by a configurable factor.

**AEL String with Placeholders:**
```
"$.login_streak > ($.account_age / ?0)"
```

**Java Usage:**
```java
// Find users whose streak is greater than their account age divided by 7
String ael = "$.login_streak > ($.account_age / ?0)";
PlaceholderValues values = PlaceholderValues.of(7);
ExpressionContext context = ExpressionContext.of(ael, values);

ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());

// Setting the resulting Expression as query filter
QueryPolicy queryPolicy = new QueryPolicy();
queryPolicy.filterExp = filter;
// ... execute query
```

## Operator Precedence

The AEL follows standard mathematical operator precedence. `*`, `/`, and `%` have higher precedence than `+` and `-`. You can use parentheses `()` to explicitly control the order of evaluation.

**Example:**
This expression:
```
"$.val1 + $.val2 * 2 > 100"
```
is evaluated as `$.val1 + ($.val2 * 2) > 100`.

To perform the addition first, use parentheses:
```
"($.val1 + $.val2) * 2 > 100"
```

By leveraging server-side arithmetic, you can build more powerful and efficient queries that are tailored to your application's business logic.