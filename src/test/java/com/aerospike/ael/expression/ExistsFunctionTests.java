package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.Value;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExistsFunctionTests {

    // ---- Bin-level exists ----

    @Test
    void binExists() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a.exists()"),
                Exp.binExists("a"));
    }

    @Test
    void twoBinExistsAnd() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a.exists() and $.b.exists()"),
                Exp.and(Exp.binExists("a"), Exp.binExists("b")));
    }

    @Test
    void binExistsOrComparison() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a.exists() or $.b > 5"),
                Exp.or(Exp.binExists("a"), Exp.gt(Exp.intBin("b"), Exp.val(5))));
    }

    @Test
    void notBinExists() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("not($.a.exists())"),
                Exp.not(Exp.binExists("a")));
    }

    @Test
    void nestedLogicalBinExists() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("($.a.exists() and $.b.exists()) or $.c > 0"),
                Exp.or(
                        Exp.and(Exp.binExists("a"), Exp.binExists("b")),
                        Exp.gt(Exp.intBin("c"), Exp.val(0))));
    }

    // ---- CDT-level exists ----

    @Test
    void mapKeyExists() {
        Exp expected = MapExp.getByKey(
                MapReturnType.EXISTS, Exp.Type.STRING,
                Exp.val("a"), Exp.mapBin("mapbin"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapbin.a.exists()"), expected);
    }

    @Test
    void listIndexExists() {
        Exp expected = ListExp.getByIndex(
                ListReturnType.EXISTS, Exp.Type.INT,
                Exp.val(0), Exp.listBin("listbin"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listbin.[0].exists()"), expected);
    }

    @Test
    void nestedMapKeyExists() {
        Exp expected = MapExp.getByKey(
                MapReturnType.EXISTS, Exp.Type.STRING,
                Exp.val("b"), Exp.mapBin("mapbin"),
                CTX.mapKey(Value.get("a")));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapbin.a.b.exists()"), expected);
    }

    // ---- Mixed expressions ----

    @Test
    void metadataAndCdtExists() {
        Exp expected = Exp.and(
                Exp.lt(Exp.ttl(), Exp.val(3600)),
                MapExp.getByKey(
                        MapReturnType.EXISTS, Exp.Type.STRING,
                        Exp.val("a"), Exp.mapBin("mapbin")));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.ttl() < 3600 and $.mapbin.a.exists()"), expected);
    }

    @Test
    void binExistsAndCdtExists() {
        Exp expected = Exp.and(
                Exp.binExists("a"),
                MapExp.getByKey(
                        MapReturnType.EXISTS, Exp.Type.STRING,
                        Exp.val("key"), Exp.mapBin("mapbin")));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a.exists() and $.mapbin.key.exists()"), expected);
    }

    @Test
    void cdtExistsInLogicalAnd() {
        Exp expected = Exp.and(
                MapExp.getByKey(
                        MapReturnType.EXISTS, Exp.Type.STRING,
                        Exp.val("a"), Exp.mapBin("mapbin")),
                Exp.gt(Exp.intBin("x"), Exp.val(0)));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapbin.a.exists() and $.x > 0"), expected);
    }

    @Test
    void notCdtExists() {
        Exp expected = Exp.not(
                MapExp.getByKey(
                        MapReturnType.EXISTS, Exp.Type.STRING,
                        Exp.val("a"), Exp.mapBin("mapbin")));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("not($.mapbin.a.exists())"), expected);
    }

    @Test
    void deeplyNestedMapExists() {
        Exp expected = MapExp.getByKey(
                MapReturnType.EXISTS, Exp.Type.STRING,
                Exp.val("c"), Exp.mapBin("m"),
                CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("b")));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.m.a.b.c.exists()"), expected);
    }

    @Test
    void existsInWhenCondition() {
        Exp expected = Exp.cond(
                Exp.binExists("mapbin"),
                MapExp.getByKey(
                        MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("a"), Exp.mapBin("mapbin")),
                Exp.val(0));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.mapbin.exists() => $.mapbin.a, default => 0)"), expected);
    }

    // ---- CDT variant coverage ----

    @Test
    void listValueExists() {
        Exp expected = ListExp.getByValue(
                ListReturnType.EXISTS,
                Exp.val(100), Exp.listBin("lb"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.lb.[=100].exists()"), expected);
    }

    @Test
    void listRankExists() {
        Exp expected = ListExp.getByRank(
                ListReturnType.EXISTS, Exp.Type.INT,
                Exp.val(-1), Exp.listBin("lb"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.lb.[#-1].exists()"), expected);
    }

    @Test
    void mapValueExists() {
        Exp expected = MapExp.getByValue(
                MapReturnType.EXISTS,
                Exp.val(100), Exp.mapBin("mb"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.{=100}.exists()"), expected);
    }

    @Test
    void mapRankExists() {
        Exp expected = MapExp.getByRank(
                MapReturnType.EXISTS, Exp.Type.STRING,
                Exp.val(0), Exp.mapBin("mb"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.{#0}.exists()"), expected);
    }

    @Test
    void mapIndexExists() {
        Exp expected = MapExp.getByIndex(
                MapReturnType.EXISTS, Exp.Type.STRING,
                Exp.val(0), Exp.mapBin("mb"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.{0}.exists()"), expected);
    }

    // ---- Edge cases ----

    @Test
    void binExistsInComparison() {
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a.exists() == true"),
                Exp.eq(Exp.binExists("a"), Exp.val(true)));
    }

    @Test
    void binExistsGtIsValidExpression() {
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a.exists() > 0"),
                Exp.gt(Exp.binExists("a"), Exp.val(0)));
    }

    // ---- Negative / syntax tests ----

    @Test
    void negativeExistsWithArgument() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.a.exists(1)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeExistsWithSpace() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.a.exists ()")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeBinExistsSpaceInParens() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.a.exists( )")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeCdtExistsSpaceInParens() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.mapbin.a.exists( )")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }
}
