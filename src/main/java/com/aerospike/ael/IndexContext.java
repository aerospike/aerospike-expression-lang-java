package com.aerospike.ael;

import lombok.Getter;

import java.util.Collection;

/**
 * This class stores namespace and indexes required to build secondary index Filter.
 * <p>
 * When {@link #querySet} is set (same value as {@code Statement.setSetName}), only {@link Index} entries whose
 * {@code setName} matches, or have no set name, are used for secondary-index selection; when it is
 * {@code null} or blank, no set-based filtering is applied.
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
    /**
     * Aerospike set name for the query (same as {@code Statement.setSetName}). When non-null, indexes are
     * filtered by each {@link Index}'s {@code setName} via {@link #indexMatchesQuerySet(Index, String)}.
     */
    private final String querySet;

    private IndexContext(String namespace, Collection<Index> indexes, String preferredBin, String querySet) {
        this.namespace = namespace;
        this.indexes = indexes;
        this.preferredBin = preferredBin;
        this.querySet = querySet;
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
        return new IndexContext(namespace, indexes, null, null);
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
            return new IndexContext(namespace, indexes, null, null);
        }
        String resolvedBin = indexes.stream()
                .filter(idx -> indexMatches(idx, namespace, indexToUse))
                .map(Index::getBin)
                .findFirst()
                .orElse(null);
        return new IndexContext(namespace, indexes, resolvedBin, null);
    }

    /**
     * Create index context with namespace, indexes, and query set.
     * Same as {@link #of(String, Collection)} but applies set-based filtering: only indexes whose
     * {@link Index} {@code setName} equals {@code querySet}, or have no set name, are used for selection.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.ael.client.query.Filter}.
     *                  Must not be null or blank
     * @param querySet  Aerospike set name for the query; null or blank disables set filtering
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext withQuerySet(String namespace, String querySet, Collection<Index> indexes) {
        validateNamespace(namespace);
        return new IndexContext(namespace, indexes, null, normalizeQuerySet(querySet));
    }

    /**
     * Create index context with query set and an index-name hint.
     * The hint is resolved only among indexes that match namespace and {@link #indexMatchesQuerySet(Index, String)}.
     *
     * @param namespace   Namespace to be used for creating {@link com.aerospike.ael.client.query.Filter}.
     *                    Must not be null or blank
     * @param querySet    Aerospike set name for the query; null or blank disables set filtering
     * @param indexes     Collection of {@link Index} objects to be used for creating Filter
     * @param indexToUse  The name of an index to use. If null, blank, or not found among eligible indexes,
     *                    index is chosen automatically by cardinality then alphabetically
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext withQuerySet(String namespace, String querySet, Collection<Index> indexes,
                                            String indexToUse) {
        validateNamespace(namespace);
        String normalized = normalizeQuerySet(querySet);
        if (indexes == null || indexToUse == null || indexToUse.isBlank()) {
            return new IndexContext(namespace, indexes, null, normalized);
        }
        String resolvedBin = indexes.stream()
                .filter(idx -> indexMatches(idx, namespace, indexToUse))
                .filter(idx -> indexMatchesQuerySet(idx, normalized))
                .map(Index::getBin)
                .findFirst()
                .orElse(null);
        return new IndexContext(namespace, indexes, resolvedBin, normalized);
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
            return new IndexContext(namespace, indexes, null, null);
        }
        boolean hasMatch = indexes.stream().anyMatch(idx -> binMatches(idx, namespace, binToUse));
        return new IndexContext(namespace, indexes, hasMatch ? binToUse : null, null);
    }

    /**
     * Create index context with a bin name hint and query set.
     * Same behavior as {@link #withBinHint(String, Collection, String)}, but the bin must exist on an index
     * that {@link #indexMatchesQuerySet(Index, String)} allows for {@code querySet}.
     *
     * @param namespace Namespace to be used for creating {@link com.aerospike.ael.client.query.Filter}.
     *                  Must not be null or blank
     * @param indexes   Collection of {@link Index} objects to be used for creating Filter
     * @param binToUse  The name of the bin whose index should be used
     * @param querySet  Aerospike set name for the query; null or blank disables set filtering
     * @return A new instance of {@code IndexContext}
     */
    public static IndexContext withBinHint(String namespace, Collection<Index> indexes, String binToUse,
                                           String querySet) {
        validateNamespace(namespace);
        String normalized = normalizeQuerySet(querySet);
        if (indexes == null || binToUse == null || binToUse.isBlank()) {
            return new IndexContext(namespace, indexes, null, normalized);
        }
        boolean hasMatch = indexes.stream()
                .anyMatch(idx -> binMatches(idx, namespace, binToUse) && indexMatchesQuerySet(idx, normalized));
        return new IndexContext(namespace, indexes, hasMatch ? binToUse : null, normalized);
    }

    private static String normalizeQuerySet(String querySet) {
        if (querySet == null || querySet.isBlank()) {
            return null;
        }
        return querySet;
    }

    /**
     * Whether {@code idx} may be used when building filters for the given query set.
     * If {@code querySet} is null, any index is eligible. Otherwise, an index with no set name is eligible;
     * otherwise its {@link Index} {@code setName} must equal {@code querySet}.
     *
     * @param idx       secondary index metadata; must not be null
     * @param querySet  normalized query set, or null to disable filtering
     * @return {@code true} if the index should be included in the catalog passed to filter selection
     */
    public static boolean indexMatchesQuerySet(Index idx, String querySet) {
        if (querySet == null) {
            return true;
        }
        String indexSet = idx.getSetName();
        if (indexSet == null || indexSet.isBlank()) {
            return true;
        }
        return querySet.equals(indexSet);
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
