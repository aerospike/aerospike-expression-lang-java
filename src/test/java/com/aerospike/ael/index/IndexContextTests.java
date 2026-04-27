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
}
