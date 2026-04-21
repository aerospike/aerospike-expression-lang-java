package com.aerospike.ael.filter;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.client.query.IndexType;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExplicitTypesFiltersTests {

    String NAMESPACE = "test1";
    List<Index> INDEXES = List.of(
            Index.builder().namespace("test1").bin("intBin1").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
            Index.builder().namespace("test1").bin("stringBin1").indexType(IndexType.STRING).binValuesRatio(1).build(),
            Index.builder().namespace("test1").bin("blobBin1").indexType(IndexType.BLOB).binValuesRatio(1).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void integerComparison() {
        // Namespace and indexes must be given to create a Filter
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > 5"), null);

        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > 5"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 6, Long.MAX_VALUE));

        TestUtils.parseFilterAndCompare(ExpressionContext.of("5 < $.intBin1.get(type: INT)"), INDEX_FILTER_INPUT,
                Filter.range("intBin1", 6, Long.MAX_VALUE));
    }

    @Test
    void stringComparison() {
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.stringBin1.get(type: STRING) == \"yes\""), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.stringBin1.get(type: STRING) == 'yes'"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare(ExpressionContext.of("\"yes\" == $.stringBin1.get(type: STRING)"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));

        TestUtils.parseFilterAndCompare(ExpressionContext.of("'yes' == $.stringBin1.get(type: STRING)"), INDEX_FILTER_INPUT,
                Filter.equal("stringBin1", "yes"));
    }

    @Test
    void stringComparisonNegativeTest() {
        // A String constant must be quoted
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("$.stringBin1.get(type: STRING) == yes")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] mismatched input '<EOF>'")
                .hasMessageContaining("at character 37");
    }

    @Test
    void blobComparison() {
        byte[] data = new byte[]{1, 2, 3};
        String encodedString = Base64.getEncoder().encodeToString(data);
        TestUtils.parseFilterAndCompare(ExpressionContext.of("$.blobBin1.get(type: BLOB) == \"" + encodedString + "\""),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));

        // Reverse
        TestUtils.parseFilterAndCompare(ExpressionContext.of("\"" + encodedString + "\" == $.blobBin1.get(type: BLOB)"),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));
    }

    @Test
    void floatComparison() {
        // No float support in secondary index filter
        assertThat(parseFilter(ExpressionContext.of("$.floatBin1.get(type: FLOAT) == 1.5"))).isNull();
        assertThat(parseFilter(ExpressionContext.of("1.5 == $.floatBin1.get(type: FLOAT)"))).isNull();
    }

    @Test
    void booleanComparison() {
        // No boolean support in secondary index filter
        assertThat(parseFilter(ExpressionContext.of("$.boolBin1.get(type: BOOL) == true"))).isNull();
        assertThat(parseFilter(ExpressionContext.of("true == $.boolBin1.get(type: BOOL)"))).isNull();
    }

    @Test
    void negativeBooleanComparison() {
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("$.boolBin1.get(type: BOOL) == 5")))
                .isInstanceOf(AelParseException.class)
                .hasMessage("Cannot compare BOOL to INT");
    }

    @Test
    void listComparison_constantOnRightSide() {
        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("$.listBin1.get(type: LIST) == [100]"))).isNull();
    }

    @Test
    void listComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("$.listBin1.get(type: LIST) == [yes, of course]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] mismatched input ','")
                .hasMessageContaining("at character 34");
    }

    @Test
    void listComparison_constantOnLeftSide() {
        assertThat(parseFilter(ExpressionContext.of("[100] == $.listBin1.get(type: LIST)"))).isNull();
    }

    @Test
    void listComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("[yes, of course] == $.listBin1.get(type: LIST)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 4");
    }

    @Test
    void mapComparison_constantOnRightSide() {
        assertThat(parseFilter(ExpressionContext.of("$.mapBin1.get(type: MAP) == {100:100}"))).isNull();
    }

    @Test
    void mapComparison_constantOnRightSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("$.mapBin1.get(type: MAP) == {yes, of course}")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] extraneous input 'yes'")
                .hasMessageContaining("at character 29");
    }

    @Test
    void mapComparison_constantOnLeftSide() {
        assertThat(parseFilter(ExpressionContext.of("{100:100} == $.mapBin1.get(type: MAP)"))).isNull();
    }

    @Test
    void mapComparison_constantOnLeftSide_NegativeTest() {
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("{yes, of course} == $.mapBin1.get(type: MAP)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 1");
    }

    @Test
    void twoStringBinsComparison() {
        assertThat(parseFilter(ExpressionContext.of("$.stringBin1.get(type: STRING) == $.stringBin2.get(type: STRING)")))
                .isNull();
    }

    @Test
    void twoIntegerBinsComparison() {
        assertThat(parseFilter(ExpressionContext.of("$.intBin1.get(type: INT) == $.intBin2.get(type: INT)"))).isNull();
    }

    @Test
    void twoFloatBinsComparison() {
        assertThat(parseFilter(ExpressionContext.of("$.floatBin1.get(type: FLOAT) == $.floatBin2.get(type: FLOAT)"))).isNull();
    }

    @Test
    void twoBlobBinsComparison() {
        assertThat(parseFilter(ExpressionContext.of("$.blobBin1.get(type: BLOB) == $.blobBin2.get(type: BLOB)"))).isNull();
    }

    @Test
    void negativeTwoDifferentBinTypesComparison() {
        assertThatThrownBy(() -> parseFilter(ExpressionContext.of("$.stringBin1.get(type: STRING) == $.floatBin2.get(type: FLOAT)")))
                .isInstanceOf(AelParseException.class)
                .hasMessage("Cannot compare STRING to FLOAT");
    }

    // ---- BLOB Literal Filter with IndexContext ----

    @Test
    void blobHexEqualityFilter() {
        byte[] data = new byte[]{(byte) 0xff, 0x00};
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.blobBin1.get(type: BLOB) == X'ff00'"),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));
    }

    @Test
    void blobHexFilterReversed() {
        byte[] data = new byte[]{(byte) 0xff, 0x00};
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("X'ff00' == $.blobBin1.get(type: BLOB)"),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));
    }

    @Test
    void b64EqualityFilter() {
        byte[] data = new byte[]{1, 2, 3};
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("$.blobBin1.get(type: BLOB) == b64'AQID'"),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));
    }

    @Test
    void b64FilterReversed() {
        byte[] data = new byte[]{1, 2, 3};
        TestUtils.parseFilterAndCompare(
                ExpressionContext.of("b64'AQID' == $.blobBin1.get(type: BLOB)"),
                INDEX_FILTER_INPUT, Filter.equal("blobBin1", data));
    }

    @Test
    void blobInequalityNoFilter() {
        TestUtils.parseAelExpressionAndCompare(
                ExpressionContext.of("$.blobBin1.get(type: BLOB) != X'ff00'"),
                null,
                Exp.ne(Exp.blobBin("blobBin1"), Exp.val(new byte[]{(byte) 0xff, 0x00})),
                INDEX_FILTER_INPUT);
    }

    @Test
    void blobOrderingNoFilter() {
        TestUtils.parseAelExpressionAndCompare(
                ExpressionContext.of("$.blobBin1.get(type: BLOB) > X'ff00'"),
                null,
                Exp.gt(Exp.blobBin("blobBin1"), Exp.val(new byte[]{(byte) 0xff, 0x00})),
                INDEX_FILTER_INPUT);
    }

    @Test
    void hexAndB64FilterEquiv() {
        byte[] data = new byte[]{1, 2, 3};
        Filter hexFilter = TestUtils.parseFilter(
                ExpressionContext.of("$.blobBin1.get(type: BLOB) == X'010203'"), INDEX_FILTER_INPUT);
        Filter b64Filter = TestUtils.parseFilter(
                ExpressionContext.of("$.blobBin1.get(type: BLOB) == b64'AQID'"), INDEX_FILTER_INPUT);
        assertThat(hexFilter).isEqualTo(b64Filter);
        assertThat(hexFilter).isEqualTo(Filter.equal("blobBin1", data));
    }
}
