# Guide: Filtering on Record Metadata

Aerospike tracks several metadata fields for each record, such as its time-to-live (TTL), last update time and so on. The Expression Language provides special functions to use this metadata in your filter expressions.

All metadata functions are called on the record root, using the `$.` prefix.

## Time-To-Live (TTL)

The TTL is the remaining life of a record in seconds. You can use it to find records that are about to expire or records that are permanent.

### `$.ttl()`

Returns the remaining TTL of the record in seconds.

**Use Case:** Find all records that will expire in the next 24 hours.

**AEL String:**
```
"$.ttl() < 86400"  // 86400 seconds = 24 hours
```

**Use Case:** Find all records that are set to never expire. The server represents this with a TTL of 0, but special care should be taken depending on server version. A common convention might be to check for a very large TTL if your application sets them. For records that are created without a TTL and the namespace has a default TTL, `ttl()` will reflect that. A record explicitly set to never expire (TTL -1 on write) will have a void time of 0, and its TTL will be calculated from that. On server versions 4.2+, a TTL of -1 can be used to signify "never expire".

**AEL String:**
To find records that will not expire (assuming server 4.2+ and TTL set to -1 on write):
```
"$.ttl() == -1"
```

## Last Update Time

You can filter records based on when they were last modified.

### `$.lastUpdate()`

Returns the timestamp of when the record was last updated, in nanoseconds since the Unix epoch (January 1, 1970).

**Use Case:** Find records updated before the year 2023.

**AEL String:**
```
// Timestamp for 2023-01-01T00:00:00Z in nanoseconds
"$.lastUpdate() < 1672531200000000000"
```

### `$.sinceUpdate()`

Returns the number of milliseconds that have passed since the record was last updated. This is often more convenient than `lastUpdate()`.

**Use Case:** Find all records that have not been modified in the last 7 days.

**AEL String:**
```
"$.sinceUpdate() > 604800000" // 7 * 24 * 60 * 60 * 1000 milliseconds
```

## Record Storage Size

You can filter records based on how much storage they consume.

### `$.deviceSize()`

Returns the amount of storage the record occupies on disk, in bytes.

**Use Case:** Find "large" records that consume more than 1 megabyte of disk space.

**AEL String:**
```
"$.deviceSize() > 1048576" // 1024 * 1024 bytes
```

### `$.memorySize()`

Returns the amount of storage the record occupies in memory, in bytes. This is relevant for hybrid storage namespaces (data in memory).

**AEL String:**
```
"$.memorySize() > 131072" // 128 KB
```

## Other Metadata Functions

### `$.isTombstone()`

Returns `true` if the record is a tombstone (i.e., it has been deleted but not yet cleaned up by the server).

**Use Case:** Find records that have been deleted but are still occupying space.

**AEL String:**
```
"$.isTombstone() == true"
```

### `$.setName()`

Returns the name of the set the record belongs to.

**Use Case:** Find records that are in either the 'customers' or 'prospects' set.

**AEL String:**
```
"$.setName() == 'customers' or $.setName() == 'prospects'"
```

### `$.digestModulo(value)`

Returns the record's digest (its unique ID) modulo some integer value. This is a powerful function for distributing work across multiple clients.

**Use Case:** Process 1/4 of the records in a batch job.

**AEL String:**
This expression will be true for roughly 25% of your records.
```
"$.digestModulo(4) == 0"
```