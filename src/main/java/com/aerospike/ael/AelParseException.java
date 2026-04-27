package com.aerospike.ael;

/**
 * Represents a general processing exception that can occur during AEL expression parsing.
 * It is typically not expected to be caught by the caller, but rather indicates a potentially
 * unrecoverable issue like invalid input, failing validation or unsupported functionality.
 */
public class AelParseException extends RuntimeException {

    public AelParseException(String description) {
        super(description);
    }

    public AelParseException(String description, Throwable cause) {
        super(description, cause);
    }
}
