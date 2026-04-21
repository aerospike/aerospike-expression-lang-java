package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.util.TestUtils;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.Value;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CastingTests {

    @Test
    void negativeInvalidTypesComparison() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.stringBin1.get(type: STRING) > $.intBin2.asFloat()")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare STRING to FLOAT");
    }

    // --- Literal casting tests ---

    @Test
    void intLiteralToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("28.asFloat() == 28.0"),
                Exp.eq(Exp.toFloat(Exp.val(28)), Exp.val(28.0)));
    }

    @Test
    void floatLiteralToInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("27.0.asInt() == 27"),
                Exp.eq(Exp.toInt(Exp.val(27.0)), Exp.val(27)));
    }

    @Test
    void negativeIntToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-5.asFloat() == -5.0"),
                Exp.eq(Exp.toFloat(Exp.val(-5)), Exp.val(-5.0)));
    }

    @Test
    void negativeFloatToInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-5.5.asInt() == -5"),
                Exp.eq(Exp.toInt(Exp.val(-5.5)), Exp.val(-5)));
    }

    @Test
    void zeroIntToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0.asFloat() == 0.0"),
                Exp.eq(Exp.toFloat(Exp.val(0)), Exp.val(0.0)));
    }

    @Test
    void zeroFloatToInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0.0.asInt() == 0"),
                Exp.eq(Exp.toInt(Exp.val(0.0)), Exp.val(0)));
    }

    @Test
    void leadingDotFloatToInt() {
        // Leading-dot float literal (.37) with cast - tests grammar ambiguity
        // between floatOperand ('.' INT) and operandCast (numberOperand '.' pathFunctionCast)
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of(".37.asInt() == 0"),
                Exp.eq(Exp.toInt(Exp.val(0.37)), Exp.val(0)));
    }

    @Test
    void longMinIntLiteralToFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-9223372036854775808.asFloat() < 0.0"),
                Exp.lt(Exp.toFloat(Exp.val(Long.MIN_VALUE)), Exp.val(0.0)));
    }

    // --- Type-validation for cast expressions ---

    @Test
    void castToFloatComparedToStringThrows() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("28.asFloat() == \"hello\"")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void castToIntComparedToStringThrows() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("28.0.asInt() == \"hello\"")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    // --- Explicit bin casting (asFloat/asInt produce Exp.toFloat/toInt wrappers) ---

    @Test
    void binAsFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.asFloat() == 1.0"),
                Exp.eq(Exp.toFloat(Exp.intBin("bin")), Exp.val(1.0)));
    }

    @Test
    void binAsInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.asInt() == 1"),
                Exp.eq(Exp.toInt(Exp.floatBin("bin")), Exp.val(1)));
    }

    @Test
    void binAsIntInComparison() {
        Exp expectedExp = Exp.gt(Exp.intBin("intBin1"), Exp.toInt(Exp.floatBin("floatBin1")));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > $.floatBin1.asInt()"), expectedExp);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > $.floatBin1.asInt()"), expectedExp);
    }

    @Test
    void binAsFloatInComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1.get(type: INT) > $.intBin2.asFloat()"),
                Exp.gt(Exp.intBin("intBin1"), Exp.toFloat(Exp.intBin("intBin2"))));
    }

    @Test
    void binAsFloatInAddition() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.binA.asFloat() + 4.0 == 5.0"),
                Exp.eq(Exp.add(Exp.toFloat(Exp.intBin("binA")), Exp.val(4.0)), Exp.val(5.0)));
    }

    @Test
    void binAsIntInSubtraction() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.binA.asInt() - 3 == 0"),
                Exp.eq(Exp.sub(Exp.toInt(Exp.floatBin("binA")), Exp.val(3)), Exp.val(0)));
    }

    // --- CDT list casting ---

    @Test
    void listIndexAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val(0), Exp.listBin("listBin1"))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].asInt() == 100"), expected);
    }

    @Test
    void listIndexAsFloat() {
        Exp expected = Exp.eq(
                Exp.toFloat(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                        Exp.val(0), Exp.listBin("listBin1"))),
                Exp.val(100.0));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].asFloat() == 100.0"), expected);
    }

    @Test
    void listValueAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(ListExp.getByValue(ListReturnType.VALUE,
                        Exp.val(100), Exp.listBin("listBin1"))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100].asInt() == 100"), expected);
    }

    @Test
    void listRankAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(ListExp.getByRank(ListReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val(-1), Exp.listBin("listBin1"))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-1].asInt() == 100"), expected);
    }

    @Test
    void listValueAsFloat() {
        Exp expected = Exp.eq(
                Exp.toFloat(ListExp.getByValue(ListReturnType.VALUE,
                        Exp.val(100), Exp.listBin("listBin1"))),
                Exp.val(100.0));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100].asFloat() == 100.0"), expected);
    }

    @Test
    void listRankAsFloat() {
        Exp expected = Exp.eq(
                Exp.toFloat(ListExp.getByRank(ListReturnType.VALUE, Exp.Type.INT,
                        Exp.val(-1), Exp.listBin("listBin1"))),
                Exp.val(100.0));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-1].asFloat() == 100.0"), expected);
    }

    // --- CDT map casting ---

    @Test
    void mapKeyAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val("a"), Exp.mapBin("mapBin1"))),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.asInt() == 200"), expected);
    }

    @Test
    void mapKeyAsFloat() {
        Exp expected = Exp.eq(
                Exp.toFloat(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                        Exp.val("a"), Exp.mapBin("mapBin1"))),
                Exp.val(200.0));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.asFloat() == 200.0"), expected);
    }

    @Test
    void mapIndexAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val(0), Exp.mapBin("mapBin1"))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0}.asInt() == 100"), expected);
    }

    @Test
    void mapIndexAsFloat() {
        Exp expected = Exp.eq(
                Exp.toFloat(MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.INT,
                        Exp.val(0), Exp.mapBin("mapBin1"))),
                Exp.val(100.0));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0}.asFloat() == 100.0"), expected);
    }

    @Test
    void mapValueAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(MapExp.getByValue(MapReturnType.VALUE,
                        Exp.val(100), Exp.mapBin("mapBin1"))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=100}.asInt() == 100"), expected);
    }

    @Test
    void mapRankAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(MapExp.getByRank(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val(-1), Exp.mapBin("mapBin1"))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-1}.asInt() == 100"), expected);
    }

    @Test
    void nestedMapRankAsInt() {
        Exp expected = Exp.eq(
                Exp.toInt(MapExp.getByRank(MapReturnType.VALUE, Exp.Type.FLOAT,
                        Exp.val(-1), Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")))),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.{#-1}.asInt() == 100"), expected);
    }

}
