package com.aerospike.ael.parts.operand;

/**
 * This interface provides an abstraction for an operand that returns a single value to be used for constructing
 * the resulting filter (e.g., a String for StringOperand or a list of objects for ListOperand)
 */
public interface ParsedValueOperand {

    Object getValue();
}
