# Guide: Working with Lists and Maps

The Expression Language provides a powerful and intuitive syntax for filtering on Complex Data Types (CDTs), such as Lists and Maps. This guide will show you how to query these structures.

The syntax allows to work with complex data filtering in a readable manner, leading to highly efficient queries.

## Accessing Map Values

You can access values within a map bin using standard dot notation.

### Filtering by Map Key

Let's say you have a `user` bin that is a map containing profile information.

**Record Data:**
```json
{
  "user": {
    "name": "Alice",
    "email": "alice@example.com",
    "logins": 150
  }
}
```

**AEL String:**
To find users with more than 100 logins, you can write:
```
"$.user.logins > 100"
```

**Java Usage:**
```java
ExpressionContext context = ExpressionContext.of("$.user.logins > 100");
// ...
```

### Accessing with Non-Standard Keys

If a map key contains spaces or special characters, you can use single quotes.

**Record Data:**
```json
{
  "metrics": {
    "daily logins": 25
  }
}
```

**AEL String:**
```
"$.metrics.'daily logins' > 20"
```

### Filtering by Map Index

Assuming we have the same map containing profile information:

**Record Data:**
```json
{
  "user": {
    "name": "Alice",
    "email": "alice@example.com",
    "logins": 150
  }
}
```

Indexes are 0-based. To access the first element, use `[0]`.

**AEL String:**
To find records with element indexed at 0 having value `Alice`, you can write it in a short form:

```
"$.user.{0} == 'Alice'"
```

Or use the full form of such AEL string if needed:

```
"$.user.{0}.get(type: STRING, return: VALUE) == 'Alice'"
```

### Filtering by Map Value

**AEL String:**
To find records having element with value 150, you can write:
```
"$.user.{=150}"
```

For instance, by using a counting function you can find if there are multiple records where user has value 150:
```
"$.user.{=150}.count() > 1"
```

### Filtering by Map Rank

Assuming we have an ordered map containing user preferences information:

**Record Data:**
```json
{
  "user": {
    "setting1": 15,
    "setting2": 150,
    "setting3": 25
  }
}
```
**AEL String:**
To find records having value of an element with rank 2 larger than 20, you can write:
```
"$.user.{#2} > 20"
```

Or in full form:
```
"$.user.{#2} > 20".get(type: INT, return: VALUE)
```

## Accessing List Elements

### Filtering by List Index

Indexes are 0-based. To access the first element, use `[0]`.

**Record Data:**
Let's say you have a `scores` list bin containing test scores.
```json
{
  "scores": [88, 95, 72]
}
```

**AEL String:**
To find records where the first score is greater than 90:
```
"$.scores.[0] > 90"
```

### Filtering by List Value

**AEL String:**
To find records with a scores element equal to 90:
```
"$.scores.[=90]"
```

For instance, you can use counting function to find if there are multiple records with value 90:
```
"$.scores.[=90].count() > 1"
```

### Filtering by List Rank

**AEL String:**
To find records where the value of scores element with rank 2 is larger than 30:
```
"$.scores.[#2] > 30"
```

## Querying Nested Structures

The real power of the AEL shines when you combine these accessors to query deeply nested data.

### Map containing a List

Imagine a `user` bin where one of the map values is a list of roles.

**Record Data:**
```json
{
  "user": {
    "name": "Bob",
    "roles": ["admin", "editor"]
  }
}
```

**AEL String:**
To find a user whose first role is "admin":
```
"$.user.roles.[0] == 'admin'"
```

### List containing a Map

Now consider a list of `devices`, where each element is a map.

**Record Data:**
```json
{
  "devices": [
    { "type": "phone", "os": "iOS" },
    { "type": "laptop", "os": "linux" }
  ]
}
```

**AEL String:**
To find records where the second device is a laptop:
```
"$.devices.[1].type == 'laptop'"
```

### Functions on CDTs

You can call functions on CDT bins, such as counting the size.

**AEL String:**
To find records where the `devices` list contains more than 1 item:
```
"$.devices.count() > 1"
```
You can also see this written as `$.devices.[].count() > 1`, which is equivalent.