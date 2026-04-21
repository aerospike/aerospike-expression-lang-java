package com.aerospike.ael.parts.operand;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.fluent.AerospikeComparator;
import com.aerospike.ael.parts.AbstractPart;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A factory for creating different types of {@link AbstractPart} operands based on a given value.
 * <p>
 * This factory provides a static method to dynamically create concrete operand implementations
 * from various Java types. It centralizes the logic for type-specific object creation.
 *
 * @see StringOperand
 * @see BooleanOperand
 * @see FloatOperand
 * @see IntOperand
 * @see ListOperand
 * @see MapOperand
 * @see BlobOperand
 */
public interface OperandFactory {

    /**
     * Creates a concrete {@link AbstractPart} operand based on the type of the provided value.
     * <p>
     * This method handles the creation of operands for common data types:
     * <ul>
     * <li>{@link String} to {@link StringOperand}.</li>
     * <li>{@link Boolean} to {@link BooleanOperand}.</li>
     * <li>{@link Float} or {@link Double} to {@link FloatOperand}.</li>
     * <li>{@link Integer} or {@link Long} to {@link IntOperand}.</li>
     * <li>{@link List} to {@link ListOperand}.</li>
     * <li>{@link Map} to {@link MapOperand}.</li>
     * <li>{@code byte[]} to {@link BlobOperand}.</li>
     * </ul>
     *
     * @param value The object to be converted into an operand. This cannot be {@code null}.
     * @return A new instance of an operand that extends {@link AbstractPart}.
     * @throws IllegalArgumentException      If the value provided is {@code null}.
     * @throws UnsupportedOperationException If the type of the value is not supported by the factory.
     */
    static AbstractPart createOperand(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot create operand from null value");
        }

        if (value instanceof String) {
            return new StringOperand((String) value);
        } else if (value instanceof Boolean) {
            return new BooleanOperand((Boolean) value);
        } else if (value instanceof Float || value instanceof Double) {
            return new FloatOperand(((Number) value).doubleValue());
        } else if (value instanceof Integer || value instanceof Long) {
            return new IntOperand(((Number) value).longValue());
        } else if (value instanceof List<?> list) {
            @SuppressWarnings("unchecked")
            List<Object> objectList = (List<Object>) list;
            return new ListOperand(objectList);
        } else if (value instanceof SortedMap<?, ?> sortedMap) {
            @SuppressWarnings("unchecked")
            SortedMap<Object, Object> objectMap = (SortedMap<Object, Object>) sortedMap;
            return new MapOperand(objectMap);
        } else if (value instanceof byte[] bytes) {
            return new BlobOperand(bytes);
        } else if (value instanceof Map<?, ?> map) {
            try {
                @SuppressWarnings("unchecked")
                Map<Object, Object> objectMap = (Map<Object, Object>) map;
                SortedMap<Object, Object> sortedMap = new TreeMap<>(new AerospikeComparator());
                sortedMap.putAll(objectMap);
                return new MapOperand(sortedMap);
            } catch (ClassCastException | NullPointerException | UnsupportedOperationException e) {
                throw new AelParseException(
                        "Map keys must be mutually comparable for operand creation", e);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Cannot create operand from value of type %s",
                    value.getClass().getSimpleName()));
        }
    }
}
