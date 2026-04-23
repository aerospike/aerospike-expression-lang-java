package com.aerospike.ael.filter;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.client.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilter;
import static com.aerospike.ael.util.TestUtils.parseFilterAndCompare;
import static org.assertj.core.api.Assertions.assertThat;

class BinFiltersTests {

    String NAMESPACE = "test1";
    List<com.aerospike.ael.Index> INDEXES = List.of(
            Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
            Index.builder().namespace(NAMESPACE).bin("stringBin1").indexType(IndexType.STRING).binValuesRatio(1).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void binGT() {
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 > 100", null), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 101, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 > -100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", -99, Long.MAX_VALUE));

        // Comparing Strings is not supported by secondary index Filters
        assertThat(parseFilter(ExpressionContext.of("$.stringBin1 > 'text'"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("$.stringBin1 > \"text\""), INDEX_FILTER_INPUT)).isNull();

        // "$.intBin1 > 100" and "100 < $.intBin1" represent identical Filters
        parseFilterAndCompare(ExpressionContext.of("100 < $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 101, Long.MAX_VALUE));

        // Comparing Strings is not supported by secondary index Filters
        assertThat(parseFilter(ExpressionContext.of("'text' > $.stringBin1"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("\"text\" > $.stringBin1"), INDEX_FILTER_INPUT)).isNull();
    }

    @Test
    void binGT_logical_combinations() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 < 1000"), IndexContext.of(NAMESPACE, indexes),
                Filter.range("intBin2", Long.MIN_VALUE, 999));

        parseFilterAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 < 1000"), null); // No indexes given
    }

    @Test
    void binGE() {
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 >= 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 100, Long.MAX_VALUE));

        // "$.intBin1 >= 100" and "100 <= $.intBin1" represent identical Filters
        parseFilterAndCompare(ExpressionContext.of("100 <= $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 100, Long.MAX_VALUE));
    }

    @Test
    void binLT() {
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 < 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 99));

        parseFilterAndCompare(ExpressionContext.of("100 > $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 99));
    }

    @Test
    void binLE() {
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 <= 100"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 100));

        parseFilterAndCompare(ExpressionContext.of("100 >= $.intBin1"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", Long.MIN_VALUE, 100));
    }

    @Test
    void binEQ() {
        parseFilterAndCompare(ExpressionContext.of("$.intBin1 == 100"), INDEX_FILTER_INPUT,
                Filter.equal("intBin1", 100));
        parseFilterAndCompare(ExpressionContext.of("100 == $.intBin1"), INDEX_FILTER_INPUT,
                Filter.equal("intBin1", 100));

        parseFilterAndCompare(ExpressionContext.of("$.stringBin1 == 'text'"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "text"));
        parseFilterAndCompare(ExpressionContext.of("$.stringBin1 == \"text\""), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "text"));
    }

    @Test
    void binNOTEQ() {
        // NOT EQUAL is not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("$.intBin1 != 100"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("$.stringBin1 != 'text'"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("$.stringBin1 != \"text\""), INDEX_FILTER_INPUT)).isNull();

        assertThat(parseFilter(ExpressionContext.of("100 != $.intBin1"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("100 != 'text'"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("100 != \"text\""), INDEX_FILTER_INPUT)).isNull();
    }

    // ---- SI filter with keyword bin name ----

    @Test
    void keywordBinNameWithSIFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("and").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        IndexContext indexCtx = IndexContext.of(NAMESPACE, indexes);
        parseFilterAndCompare(ExpressionContext.of("$.and == 1"), indexCtx,
                Filter.equal("and", 1));
    }
}
