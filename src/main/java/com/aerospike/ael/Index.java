package com.aerospike.ael;

import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.query.IndexCollectionType;
import com.aerospike.ael.client.query.IndexType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * This class represents a secondary index created in the cluster.
 * <p>
 * Mandatory fields: {@code namespace}, {@code bin}, {@code indexType}.
 * These are validated on build and must not be null/blank (for strings).
 * {@code binValuesRatio} defaults to 0 if not set and must not be negative.
 */
@Builder
@EqualsAndHashCode
@Getter
public class Index {

    /**
     * Namespace of the indexed bin
     */
    private final String namespace;
    /**
     * Name of the indexed bin
     */
    private final String bin;
    /**
     * Name of the index
     */
    private final String name;
    /**
     * {@link IndexType} of the index
     */
    private final IndexType indexType;
    /**
     * Cardinality of the index calculated using "sindex-stat" command and looking at the ratio of entries
     * to unique bin values for the given secondary index on the node (entries_per_bval)
     */
    private final int binValuesRatio;
    /**
     * {@link IndexCollectionType} of the index
     */
    private final IndexCollectionType indexCollectionType;
    /**
     * Array of {@link CTX} representing context of the index
     */
    private final CTX[] ctx;

    public Index(String namespace, String bin, String name, IndexType indexType, int binValuesRatio,
                 IndexCollectionType indexCollectionType, CTX[] ctx) {
        validateMandatory(namespace, bin, indexType, binValuesRatio);
        this.namespace = namespace;
        this.bin = bin;
        this.name = name;
        this.indexType = indexType;
        this.binValuesRatio = binValuesRatio;
        this.indexCollectionType = indexCollectionType;
        this.ctx = ctx;
    }

    private static void validateMandatory(String namespace, String bin, IndexType indexType, int binValuesRatio) {
        requireNonBlank(namespace, "namespace");
        requireNonBlank(bin, "bin");
        requireNonNull(indexType, "indexType");
        if (binValuesRatio < 0) {
            throw new IllegalArgumentException("binValuesRatio must not be negative");
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
    }
}
