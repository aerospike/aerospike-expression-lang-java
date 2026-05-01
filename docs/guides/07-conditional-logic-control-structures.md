# Conditional Logic: Control Structures `let` and  `when`

The Expression Language supports conditional logic through `when` and `let` control structures, similar to a `CASE` statement in SQL. This allows you to build sophisticated expressions that can return different values based on a set of conditions.

This is particularly useful for server-side data transformation or implementing complex business rules.

## Control Structure `when`

The `when` structure enables you to push complex conditional logic directly to the server, reducing the need to pull data to the client for evaluation and minimizing data transfer.

The basic structure of a `when` expression is a series of `condition => result` pairs, optionally ending with a `default` clause:

```
when(
    condition1 => result1,
    condition2 => result2,
    ...,
    default => defaultResult
)
```

The server evaluates the conditions in order and returns the result for the *first* condition that evaluates to `true`. If no conditions are true, the `default` result is returned.

## Use Case: Tiered Logic

Imagine you want to categorize users into different tiers based on their `rank` bin, and then check if their `tier` bin matches that calculated category.

**Business Rules:**
*   If `rank` > 90, tier is "gold"
*   If `rank` > 70, tier is "silver"
*   If `rank` > 40, tier is "bronze"
*   Otherwise, the tier is "basic"

### AEL String

You can express this logic in a single AEL expression to verify a user's `tier`.

```
"$.tier == when($.rank > 90 => 'gold', $.rank > 70 => 'silver', $.rank > 40 => 'bronze', default => 'basic')"
```

Let's break this down:
1.  `when(...)` evaluates the inner logic first. If a record has `rank: 95`, the `when` block returns the string `'gold'`.
2.  The outer expression then becomes `$.tier == 'gold'`.
3.  The entire expression will return `true` if the `tier` bin for that record is indeed set to "gold", and `false` otherwise.

### Using Static AEL String in Java

```java
String aelString = "$.tier == when($.rank > 90 => 'gold', $.rank > 70 => 'silver', $.rank > 40 => 'bronze', default => 'basic')";

ExpressionContext context = ExpressionContext.of(aelString);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());

// Using Aerospike Java client
QueryPolicy queryPolicy = new QueryPolicy();

// Setting the resulting Expression as query filter
queryPolicy.filterExp = filter;
// This query will now return only the records where the tier bin is correctly set according to the rank.
```

### Using AEL String with Placeholders in Java

We can also use placeholders within a `when` expression for greater flexibility. Placeholders mark the places where values provided separately are matched by indexes.
This way the same AEL String can be used multiple times with different values for the same placeholders.

For example, let's add placeholders to our previous AEL expression and use the same API for generating an `Expression`:

```java
String ael = "$.tier == when($.rank > ?0 => ?1, $.rank > ?2 => ?3, default => ?4)";

PlaceholderValues values = PlaceholderValues.of(
        90, "gold",
        70, "silver",
        40, "bronze",
        "basic"
);

ExpressionContext context = ExpressionContext.of(ael, values);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());
// ...
```

The `when` structure enables you to push complex conditional logic directly to the server, reducing the need to pull data to the client for evaluation and minimizing data transfer.

## Error Handling: `unknown` and `error`

The `unknown` keyword produces an expression that throws a server-side exception whenever it is evaluated. While primarily useful in `when` branches to signal that a particular condition should never occur, `unknown`/`error` is syntactically valid in any expression position (e.g., `$.a == unknown`, `unknown + 1`). Outside a guarded `when` branch, it will cause a server-side exception on evaluation.

The `error` keyword is syntactic sugar for `unknown` -- both compile to the same underlying expression (`Exp.unknown()`). Use whichever reads better in your context: `error` may be clearer when the intent is to signal a failure, while `unknown` matches the underlying Aerospike Exp API name.

### Example: Fail on Default

Return a map bin if its size exceeds 5, otherwise throw a server-side exception:

```
when($.mapBin.{}.count() > 5 => $.mapBin, default => unknown)
```

Or equivalently with `error`:

```
when($.mapBin.{}.count() > 5 => $.mapBin, default => error)
```

### Example: Fail on Specific Branch

You can also use `unknown`/`error` in a non-default branch:

```
when($.status == 0 => error, $.status == 1 => 'active', default => 'inactive')
```

### Using as a Bin Name

Since `unknown` and `error` are reserved keywords, they are interpreted as expressions (not bin names) when used without the `$.` prefix. To reference bins named `unknown` or `error`, use the `$.` prefix:

```
$.unknown == 5
$.error == 10
```

Quoted forms are also supported and equivalent: `$.'unknown'`, `$."error"`.

## Control Structure `let`

The basic structure of a let expression allows you to declare temporary variables and use them within a subsequent expression:

```
let (
    var1 = val1,
    var2 = val2,
    ...
) then (expression)

```

The server evaluates the variable assignments in order, making each variable available for use in later assignments and in the final expression after the `then` keyword.

## Simple Example

For a simpler illustration of variable usage and calculations:

```
"let (x = 1, y = ${x} + 1) then (${x} + ${y})"
```

This expression:

1. Defines variable x with value 1
2. Defines variable y with value ${x} + 1 (which evaluates to 2)
3. Returns the result of ${x} + ${y} (which evaluates to 3)

The `let` structure enables us to create more readable and maintainable expressions by breaking complex logic into named variables. This is especially valuable when the same intermediate calculation is used multiple times in an expression or when the expression logic is complex.

## Use Case: More Complex Calculations

Imagine we want to calculate a user's eligibility score based on multiple factors like account `age`, `transaction history`, and `credit score`, then determine if they qualify for a premium service.

**Business Rules:**
*   Calculate a base score from account age
*   Add bonus points based on transaction count
*   Apply a multiplier based on credit score
*   User qualifies if final score exceeds threshold

### AEL String

We can use the `let` construct to make this complex calculation more readable and maintainable:

```
"let (
    baseScore = $.accountAgeMonths * 0.5,
    transactionBonus = $.transactionCount > 100 ? 25 : 0,
    creditMultiplier = $.creditScore > 700 ? 1.5 : 1.0,
    finalScore = (${baseScore} + ${transactionBonus}) * ${creditMultiplier}
) then (${finalScore} >= 75 && $.premiumEligible == true)"
```

Let's break this down:

1. We first calculate `baseScore` based on account age
2. We determine `transactionBonus` based on transaction count
3. We set `creditMultiplier` based on credit score
4. We calculate the `finalScore` using the previous variables
5. The final expression checks if the `finalScore` is at least 75 and if `premiumEligible` is true


### Using Static AEL String in Java

```java
String aelString = "let (" +
        "baseScore = $.accountAgeMonths * 0.5, " +
        "transactionBonus = $.transactionCount > 100 ? 25 : 0, " +
        "creditMultiplier = $.creditScore > 700 ? 1.5 : 1.0, " +
        "finalScore = (${baseScore} + ${transactionBonus}) * ${creditMultiplier}" +
        ") then (${finalScore} >= 75 && $.premiumEligible == true)";

ExpressionContext context = ExpressionContext.of(aelString);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());

// Using Aerospike Java client
QueryPolicy queryPolicy = new QueryPolicy();

// Setting the resulting Expression as query filter
queryPolicy.filterExp = filter;
// This query will return only records of users who qualify for premium service
```

### Using AEL String with Placeholders in Java

You can also use placeholders within a let expression for greater flexibility. Placeholders mark the places where values provided separately are matched by indexes.
This way the same AEL String can be used multiple times with different values for the same placeholders.

For example, let's add placeholders to our previous AEL expression and use the same API for generating an `Expression`:

```java
String ael = "let (" +
        "baseScore = $.accountAgeMonths * ?0, " +
        "transactionThreshold = ?1, " +
        "transactionBonus = $.transactionCount > ${transactionThreshold} ? ?2 : 0, " +
        "creditThreshold = ?3, " +
        "creditMultiplier = $.creditScore > ${creditThreshold} ? ?4 : 1.0, " +
        "finalScore = (${baseScore} + ${transactionBonus}) * ${creditMultiplier}" +
        ") then (${finalScore} >= ?5 && $.premiumEligible == true)";

PlaceholderValues values = PlaceholderValues.of(
        0.5,    // Age multiplier
        100,    // Transaction threshold
        25,     // Transaction bonus
        700,    // Credit score threshold
        1.5,    // Credit multiplier
        75      // Minimum score threshold
);

ExpressionContext context = ExpressionContext.of(ael, values);
ParsedExpression parsed = parser.parseExpression(context);
Expression filter = Exp.build(parsed.getResult().getExp());
// ...
```