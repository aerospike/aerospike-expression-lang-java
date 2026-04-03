package com.aerospike.dsl.expression;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.Expression;
import com.aerospike.dsl.impl.DSLParserImpl;
import org.junit.jupiter.api.Test;

import static com.aerospike.dsl.util.TestUtils.parseCtx;
import static com.aerospike.dsl.util.TestUtils.parseDslAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that bare {@code .get()} (no parameters) is an identity operation:
 * appending it to any path produces the same expression as the path alone.
 */
class BareGetFunctionTests {

    private static final DSLParserImpl parser = new DSLParserImpl();

    // --- Plain bin paths ---

    @Test
    void binComparison() {
        parseDslAndCompare("$.intBin1.get() != 100", "$.intBin1 != 100");
    }

    @Test
    void binComparisonBothSides() {
        parseDslAndCompare("$.intBin1.get() > $.intBin2.get()", "$.intBin1 > $.intBin2");
    }

    // --- List CDT paths ---

    @Test
    void listByIndex() {
        parseDslAndCompare("$.listBin1.[0].get() == 100", "$.listBin1.[0] == 100");
    }

    @Test
    void listByValue() {
        parseDslAndCompare("$.listBin1.[=100].get() == 100", "$.listBin1.[=100] == 100");
    }

    @Test
    void listByRank() {
        parseDslAndCompare("$.listBin1.[#-1].get() == 100", "$.listBin1.[#-1] == 100");
    }

    // --- Map CDT paths ---

    @Test
    void mapByKey() {
        parseDslAndCompare("$.mapBin1.a.get() == 100", "$.mapBin1.a == 100");
    }

    @Test
    void mapByIndex() {
        parseDslAndCompare("$.mapBin1.{0}.get() == 100", "$.mapBin1.{0} == 100");
    }

    @Test
    void mapByValue() {
        parseDslAndCompare("$.mapBin1.{=100}.get() == 100", "$.mapBin1.{=100} == 100");
    }

    @Test
    void mapByRank() {
        parseDslAndCompare("$.mapBin1.{#-1}.get() == 100", "$.mapBin1.{#-1} == 100");
    }

    // --- Nested and mixed CDT paths ---

    @Test
    void nestedListIndexes() {
        parseDslAndCompare(
                "$.listBin1.[0].[0].[0].get() == 100",
                "$.listBin1.[0].[0].[0] == 100");
    }

    @Test
    void nestedMapKeys() {
        parseDslAndCompare(
                "$.mapBin1.a.bb.bcc.get() == 100",
                "$.mapBin1.a.bb.bcc == 100");
    }

    @Test
    void mapToList() {
        parseDslAndCompare(
                "$.mapBin1.a.cc.[2].get() > 100",
                "$.mapBin1.a.cc.[2] > 100");
    }

    @Test
    void listToMap() {
        parseDslAndCompare(
                "$.listBin1.[2].cc.get() > 100",
                "$.listBin1.[2].cc > 100");
    }

    // --- Compound expressions ---

    @Test
    void arithmetic() {
        parseDslAndCompare(
                "($.intBin1.get() + $.intBin2) > 10",
                "($.intBin1 + $.intBin2) > 10");
    }

    @Test
    void logical() {
        parseDslAndCompare(
                "$.intBin1.get() > 5 and $.intBin2.get() < 10",
                "$.intBin1 > 5 and $.intBin2 < 10");
    }

    @Test
    void inExpression() {
        parseDslAndCompare(
                "$.intBin1.get() in [1, 2, 3]",
                "$.intBin1 in [1, 2, 3]");
    }

    @Test
    void placeholder() {
        PlaceholderValues pv = PlaceholderValues.of(42);
        Expression actual = Exp.build(
                parser.parseExpression(ExpressionContext.of("$.intBin1.get() > ?0", pv)).getResult().getExp());
        Expression expected = Exp.build(
                parser.parseExpression(ExpressionContext.of("$.intBin1 > ?0", pv)).getResult().getExp());
        assertEquals(expected, actual);
    }

    // --- parseCTX rejection (negative) ---

    @Test
    void negParseCtxRejectsBareGet() {
        assertThatThrownBy(() -> parseCtx("$.listBin1.[0].get()"))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("Could not parse the given DSL path input")
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasStackTraceContaining("Path function is unsupported");
    }
}
