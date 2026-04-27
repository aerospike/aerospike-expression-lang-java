package com.aerospike.ael;

import lombok.Getter;

import java.util.Collection;

/**
 * This class stores namespace and indexes required to build secondary index Filter
 */
@Getter
public class IndexContext {

    /**
     * Namespace to be used for creating secondary index Filter. Is matched with namespace of indexes
     */
    private final String namespace;
    /**
     * Collection of {@link Index} objects to be used for creating secondary index Filter.
     * Namespace of indexes is matched with the given {@link #namespace}, bin name and index type are matched
     * with bins in AEL String
     */
    private final Collection<Index> indexes;
    /**
     * Preferred bin name derived from a user-supplied hint (index name or bin name).
     * {@code null} when no hint was provided or the hint could not be resolved.
     * Used by the filter selection algorithm to prefer an index on this bin before
     * falling back to cardinality-based selection.
     */
    private final String preferredBin;

    private IndexContext(String namespace, Collection<Index> indexes, String preferredBin) {
        this.namespace = namespace;
        this.indexes = indexes;
        this.preferredBin = preferredBin;
    }

    /**
     * Create index context with namespace and indexes.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.ael.client.query.Filter}.
     *                  Must not be null or blank
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes) {
        validateNamespace(namespace);
        return new IndexContext(namespace, indexes, null);
    }

    /**
     * Create index context specifying the index to be used.
     * The named index's bin is stored as a preference hint; the full index collection
     * is kept so the selection algorithm can fall back automatically when the hint
     * cannot be applied (e.g. type mismatch).
     *
     * @param namespace  Namespace to be used for creating {@link com.aerospike.ael.client.query.Filter}.
     *                   Must not be null or blank
     * @param indexes    Collection of {@link Index} objects to be used for creating Filter
     * @param indexToUse The name of an index to use. If null, blank, or not found in the collection,
     *                   index is chosen automatically by cardinality then alphabetically
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext of(String namespace, Collection<Index> indexes, String indexToUse) {
        validateNamespace(namespace);
        if (indexes == null || indexToUse == null || indexToUse.isBlank()) {
            return new IndexContext(namespace, indexes, null);
        }
        String resolvedBin = indexes.stream()
                .filter(idx -> indexMatches(idx, namespace, indexToUse))
                .map(Index::getBin)
                .findFirst()
                .orElse(null);
        return new IndexContext(namespace, indexes, resolvedBin);
    }

    /**
     * Create index context with a bin name hint specifying which bin's index to use.
     * The full index collection is kept; the hint is stored as a preference so the
     * selection algorithm can prefer an index on this bin. When the hint cannot be
     * applied (e.g. type mismatch or hinted bin not in query), the algorithm falls
     * back to cardinality-based selection across all indexes automatically.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.ael.client.query.Filter}.
     *                  Must not be null or blank
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @param binToUse  The name of the bin whose index should be used. If not found, null, or blank,
     *                  index is chosen automatically across all indexes
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext withBinHint(String namespace, Collection<Index> indexes, String binToUse) {
        validateNamespace(namespace);
        if (indexes == null || binToUse == null || binToUse.isBlank()) {
            return new IndexContext(namespace, indexes, null);
        }
        boolean hasMatch = indexes.stream().anyMatch(idx -> binMatches(idx, namespace, binToUse));
        return new IndexContext(namespace, indexes, hasMatch ? binToUse : null);
    }

    private static void validateNamespace(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("namespace must not be null");
        }
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
    }

    private static boolean binMatches(Index idx, String namespace, String binToUse) {
        if (idx == null || binToUse == null) {
            return false;
        }

        String binName = idx.getBin();
        if (binName == null || !binName.equals(binToUse)) {
            return false;
        }

        return namespace.equals(idx.getNamespace());
    }

    private static boolean indexMatches(Index idx, String namespace, String indexToUse) {
        if (idx == null || indexToUse == null) {
            return false;
        }

        String indexName = idx.getName();
        if (indexName == null || !indexName.equals(indexToUse)) {
            return false;
        }

        return namespace.equals(idx.getNamespace());
    }
}
