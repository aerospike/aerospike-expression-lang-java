package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.Expression;
import com.aerospike.ael.impl.AelParserImpl;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseCtx;
import static com.aerospike.ael.util.TestUtils.parseAelAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that bare {@code .get()} (no parameters) is an identity operation:
 * appending it to any path produces the same expression as the path alone.
 */
class BareGetFunctionTests {

    private static final AelParserImpl parser = new AelParserImpl();

    // --- Plain bin paths ---

    @Test
    void binComparison() {
        parseAelAndCompare("$.intBin1.get() != 100", "$.intBin1 != 100");
    }

    @Test
    void binComparisonBothSides() {
        parseAelAndCompare("$.intBin1.get() > $.intBin2.get()", "$.intBin1 > $.intBin2");
    }

    // --- List CDT paths ---

    @Test
    void listByIndex() {
        parseAelAndCompare("$.listBin1.[0].get() == 100", "$.listBin1.[0] == 100");
    }

    @Test
    void listByValue() {
        parseAelAndCompare("$.listBin1.[=100].get() == 100", "$.listBin1.[=100] == 100");
    }

    @Test
    void listByRank() {
        parseAelAndCompare("$.listBin1.[#-1].get() == 100", "$.listBin1.[#-1] == 100");
    }

    // --- Map CDT paths ---

    @Test
    void mapByKey() {
        parseAelAndCompare("$.mapBin1.a.get() == 100", "$.mapBin1.a == 100");
    }

    @Test
    void mapByIndex() {
        parseAelAndCompare("$.mapBin1.{0}.get() == 100", "$.mapBin1.{0} == 100");
    }

    @Test
    void mapByValue() {
        parseAelAndCompare("$.mapBin1.{=100}.get() == 100", "$.mapBin1.{=100} == 100");
    }

    @Test
    void mapByRank() {
        parseAelAndCompare("$.mapBin1.{#-1}.get() == 100", "$.mapBin1.{#-1} == 100");
    }

    // --- Nested and mixed CDT paths ---

    @Test
    void nestedListIndexes() {
        parseAelAndCompare(
                "$.listBin1.[0].[0].[0].get() == 100",
                "$.listBin1.[0].[0].[0] == 100");
    }

    @Test
    void nestedMapKeys() {
        parseAelAndCompare(
                "$.mapBin1.a.bb.bcc.get() == 100",
                "$.mapBin1.a.bb.bcc == 100");
    }

    @Test
    void mapToList() {
        parseAelAndCompare(
                "$.mapBin1.a.cc.[2].get() > 100",
                "$.mapBin1.a.cc.[2] > 100");
    }

    @Test
    void listToMap() {
        parseAelAndCompare(
                "$.listBin1.[2].cc.get() > 100",
                "$.listBin1.[2].cc > 100");
    }

    // --- Compound expressions ---

    @Test
    void arithmetic() {
        parseAelAndCompare(
                "($.intBin1.get() + $.intBin2) > 10",
                "($.intBin1 + $.intBin2) > 10");
    }

    @Test
    void logical() {
        parseAelAndCompare(
                "$.intBin1.get() > 5 and $.intBin2.get() < 10",
                "$.intBin1 > 5 and $.intBin2 < 10");
    }

    @Test
    void inExpression() {
        parseAelAndCompare(
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
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasStackTraceContaining("Path function is unsupported");
    }
}
