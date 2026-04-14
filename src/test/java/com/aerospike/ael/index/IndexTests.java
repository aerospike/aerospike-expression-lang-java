package com.aerospike.ael.index;

import com.aerospike.ael.Index;
import com.aerospike.ael.client.query.IndexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexTests {

    private static final String NAMESPACE = "test";
    private static final String BIN = "bin1";

    @Test
    void build_rejects_null_namespace() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(null)
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void build_rejects_blank_namespace() {
        assertThatThrownBy(() -> Index.builder()
                .namespace("   ")
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be blank");
    }

    @Test
    void build_rejects_null_bin() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(NAMESPACE)
                .bin(null)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bin must not be null");
    }

    @Test
    void build_rejects_blank_bin() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(NAMESPACE)
                .bin("")
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bin must not be blank");
    }

    @Test
    void build_rejects_null_indexType() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(NAMESPACE)
                .bin(BIN)
                .indexType(null)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("indexType must not be null");
    }

    @Test
    void build_rejects_negative_binValuesRatio() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(NAMESPACE)
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(-1)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("binValuesRatio must not be negative");
    }

    @Test
    void build_fails_when_namespace_omitted() {
        assertThatThrownBy(() -> Index.builder()
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("namespace must not be null");
    }

    @Test
    void build_fails_when_bin_omitted() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(NAMESPACE)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bin must not be null");
    }

    @Test
    void build_fails_when_indexType_omitted() {
        assertThatThrownBy(() -> Index.builder()
                .namespace(NAMESPACE)
                .bin(BIN)
                .binValuesRatio(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("indexType must not be null");
    }

    @Test
    void build_defaults_to_zero_when_binValuesRatio_omitted() {
        Index index = Index.builder()
                .namespace(NAMESPACE)
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .build();

        assertThat(index.getBinValuesRatio()).isZero();
    }

    @Test
    void build_accepts_zero_binValuesRatio() {
        Index index = Index.builder()
                .namespace(NAMESPACE)
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build();

        assertThat(index.getBinValuesRatio()).isZero();
    }

    @Test
    void build_succeeds_with_all_mandatory() {
        Index index = Index.builder()
                .namespace(NAMESPACE)
                .bin(BIN)
                .indexType(IndexType.STRING)
                .binValuesRatio(1)
                .build();

        assertThat(index.getNamespace()).isEqualTo(NAMESPACE);
        assertThat(index.getBin()).isEqualTo(BIN);
        assertThat(index.getIndexType()).isEqualTo(IndexType.STRING);
        assertThat(index.getBinValuesRatio()).isOne();
        assertThat(index.getSetName()).isNull();
    }

    @Test
    void build_accepts_optional_set_name() {
        Index index = Index.builder()
                .namespace(NAMESPACE)
                .setName("mySet")
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build();

        assertThat(index.getSetName()).isEqualTo("mySet");
    }

    @Test
    void build_blank_set_name_normalized_to_null() {
        Index index = Index.builder()
                .namespace(NAMESPACE)
                .setName("   ")
                .bin(BIN)
                .indexType(IndexType.NUMERIC)
                .binValuesRatio(0)
                .build();

        assertThat(index.getSetName()).isNull();
    }
}
