package com.aerospike.ael.visitor;

import com.aerospike.ael.client.query.Filter;

/**
 * Indicates that no applicable {@link Filter} could be generated for a given AEL expression. For internal use.
 *
 * <p>This exception is typically thrown when attempting to create a Filter for a AEL expression
 * but the structure or types of the expression do not match any supported filtering patterns
 * (e.g., comparing Strings using arithmetical operations, using OR-combined expression etc.).
 * It signifies that while the expression might be valid in a broader context, it cannot be represented with a
 * secondary index Filter.
 */
class NoApplicableFilterException extends RuntimeException {

    NoApplicableFilterException(String description) {
        super(description);
    }
}
