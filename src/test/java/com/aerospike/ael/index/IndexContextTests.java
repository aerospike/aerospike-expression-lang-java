package com.aerospike.ael.index;

import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.client.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexContextTests {

    private static final String NAMESPACE = "test";
    private static final Index VALID_INDEX = Index.builder()
            .namespace(NAMESPACE)
            .bin("bin1")
            .indexType(IndexType.NUMERIC)
            .binValuesRatio(0)
            .build();
    private static final Index VALID_NAMED_INDEX = Index.builder()
            .namespace(NAMESPACE)
            .bin("bin1")
            .name("idx_bin1")
            .indexType(IndexType.NUMERIC)
            .binValuesRatio(0)
            .build();

    @Test
    void of_rejects_null_namespace() {
        assertThatThrownBy(() -> IndexContext.of(null, List.of(VALID_INDEX)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void of_rejects_blank_namespace() {
        assertThatThrownBy(() -> IndexContext.of("  ", List.of(VALID_INDEX)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void of_accepts_valid_namespace() {
        IndexContext ctx = IndexContext.of(NAMESPACE, Collections.emptyList());

        assertThat(ctx.getNamespace()).isEqualTo(NAMESPACE);
        assertThat(ctx.getIndexes()).isEmpty();
        assertThat(ctx.getQuerySet()).isNull();
    }

    @Test
    void of_3arg_rejects_null_namespace() {
        assertThatThrownBy(() -> IndexContext.of(null, List.of(VALID_INDEX), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void of_3arg_rejects_blank_namespace() {
        assertThatThrownBy(() -> IndexContext.of("", List.of(VALID_INDEX), "idx1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void of_3arg_null_index_name_no_preferred_bin() {
        Collection<Index> indexes = List.of(VALID_NAMED_INDEX);
        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, null);

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void of_3arg_empty_index_name_no_preferred_bin() {
        Collection<Index> indexes = List.of(VALID_NAMED_INDEX);
        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, "");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void of_3arg_match_sets_preferred_bin() {
        Index other = Index.builder().namespace(NAMESPACE).bin("bin2").name("idx_bin2")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(VALID_NAMED_INDEX, other);

        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, "idx_bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isEqualTo("bin1");
    }

    @Test
    void of_3arg_no_match_no_preferred_bin() {
        Collection<Index> indexes = List.of(VALID_NAMED_INDEX);
        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, "idx_nonExistent");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void of_3arg_blank_index_name_returns_all_indexes() {
        Index blankNamedIndex = Index.builder().namespace(NAMESPACE).bin("bin1").name("  ")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(blankNamedIndex);

        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, "  ");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void of_3arg_null_indexes_does_not_throw() {
        assertThatCode(() -> IndexContext.of(NAMESPACE, null, "idx_bin1"))
                .doesNotThrowAnyException();
    }

    @Test
    void of_3arg_namespace_mismatch_returns_all_indexes() {
        Index wrongNs = Index.builder().namespace("other_ns").bin("bin1").name("idx_bin1")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(wrongNs);

        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, "idx_bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_null_indexes_does_not_throw() {
        assertThatCode(() -> IndexContext.withBinHint(NAMESPACE, null, "bin1"))
                .doesNotThrowAnyException();
    }

    @Test
    void withBinHint_rejects_null_namespace() {
        assertThatThrownBy(() -> IndexContext.withBinHint(null, List.of(VALID_INDEX), "bin1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void withBinHint_rejects_blank_namespace() {
        assertThatThrownBy(() -> IndexContext.withBinHint("", List.of(VALID_INDEX), "bin1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void withBinHint_null_bin_returns_all_indexes() {
        Collection<Index> indexes = List.of(VALID_INDEX);
        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, null);

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_empty_bin_returns_all_indexes() {
        Collection<Index> indexes = List.of(VALID_INDEX);
        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void withBinHint_match_sets_preferred_bin() {
        Index other = Index.builder().namespace(NAMESPACE).bin("bin2")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(VALID_INDEX, other);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isEqualTo("bin1");
    }

    @Test
    void withBinHint_multiple_matches_sets_preferred_bin() {
        Index second = Index.builder().namespace(NAMESPACE).bin("bin1")
                .indexType(IndexType.STRING).binValuesRatio(5).build();
        Index other = Index.builder().namespace(NAMESPACE).bin("bin2")
                .indexType(IndexType.NUMERIC).binValuesRatio(10).build();
        Collection<Index> indexes = List.of(VALID_INDEX, second, other);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isEqualTo("bin1");
    }

    @Test
    void withBinHint_no_match_no_preferred_bin() {
        Collection<Index> indexes = List.of(VALID_INDEX);
        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "nonExistentBin");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void withBinHint_namespace_mismatch_returns_all_indexes() {
        Index wrongNs = Index.builder().namespace("other_ns").bin("bin1")
                .indexType(IndexType.NUMERIC).binValuesRatio(0).build();
        Collection<Index> indexes = List.of(wrongNs);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "bin1");

        assertThat(ctx.getIndexes()).containsExactlyElementsOf(indexes);
    }

    @Test
    void resolves_preferred_bin_by_index_name_when_indexes_differ_by_set_name() {
        Index idxSet = Index.builder().namespace(NAMESPACE).setName("set").bin("age").name("ageidx")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        Index idxScan = Index.builder().namespace(NAMESPACE).setName("testScan").bin("age").name("age_idx")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        List<Index> indexes = List.of(idxSet, idxScan);

        IndexContext ctx = IndexContext.of(NAMESPACE, indexes, "age_idx");

        assertThat(ctx.getPreferredBin()).isEqualTo("age");
    }

    @Test
    void withQuerySet_stores_query_set() {
        IndexContext ctx = IndexContext.withQuerySet(NAMESPACE, "testScan", List.of(VALID_INDEX));

        assertThat(ctx.getQuerySet()).isEqualTo("testScan");
        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void withQuerySet_blank_normalized_to_null() {
        IndexContext ctx = IndexContext.withQuerySet(NAMESPACE, "  ", List.of(VALID_INDEX));

        assertThat(ctx.getQuerySet()).isNull();
    }

    @Test
    void withQuerySet_4arg_resolves_hint_only_for_matching_set() {
        Index idxSet = Index.builder().namespace(NAMESPACE).setName("set").bin("age").name("ageidx")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        Index idxScan = Index.builder().namespace(NAMESPACE).setName("testScan").bin("age").name("age_idx")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        List<Index> indexes = List.of(idxSet, idxScan);

        IndexContext ctx = IndexContext.withQuerySet(NAMESPACE, "testScan", indexes, "age_idx");

        assertThat(ctx.getQuerySet()).isEqualTo("testScan");
        assertThat(ctx.getPreferredBin()).isEqualTo("age");
    }

    @Test
    void withQuerySet_4arg_wrong_set_does_not_resolve_index_name() {
        Index idxSet = Index.builder().namespace(NAMESPACE).setName("set").bin("age").name("ageidx")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        List<Index> indexes = List.of(idxSet);

        IndexContext ctx = IndexContext.withQuerySet(NAMESPACE, "testScan", indexes, "ageidx");

        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void withBinHint_4arg_requires_bin_on_matching_set() {
        Index idxSet = Index.builder().namespace(NAMESPACE).setName("set").bin("age")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        List<Index> indexes = List.of(idxSet);

        IndexContext ctx = IndexContext.withBinHint(NAMESPACE, indexes, "age", "testScan");

        assertThat(ctx.getPreferredBin()).isNull();
    }

    @Test
    void indexMatchesQuerySet_null_query_set_matches_all() {
        Index idx = Index.builder().namespace(NAMESPACE).setName("set").bin("age")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        assertThat(IndexContext.indexMatchesQuerySet(idx, null)).isTrue();
    }

    @Test
    void indexMatchesQuerySet_blank_index_set_matches_any() {
        Index idx = Index.builder().namespace(NAMESPACE).bin("age")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        assertThat(IndexContext.indexMatchesQuerySet(idx, "testScan")).isTrue();
    }

    @Test
    void indexMatchesQuerySet_equal_set_matches() {
        Index idx = Index.builder().namespace(NAMESPACE).setName("testScan").bin("age")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        assertThat(IndexContext.indexMatchesQuerySet(idx, "testScan")).isTrue();
    }

    @Test
    void indexMatchesQuerySet_mismatched_set_excluded() {
        Index idx = Index.builder().namespace(NAMESPACE).setName("set").bin("age")
                .indexType(IndexType.NUMERIC).binValuesRatio(1).build();
        assertThat(IndexContext.indexMatchesQuerySet(idx, "testScan")).isFalse();
    }
}
