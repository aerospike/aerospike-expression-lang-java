package com.aerospike.ael.expression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

public class ImplicitTypesTests {

    @Test
    void floatComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.floatBin1 >= 100.25"),
                Exp.ge(Exp.floatBin("floatBin1"), Exp.val(100.25)));
    }

    @Test
    void booleanComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.boolBin1 == true"),
                Exp.eq(Exp.boolBin("boolBin1"), Exp.val(true)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("false == $.boolBin1"),
                Exp.eq(Exp.val(false), Exp.boolBin("boolBin1")));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.boolBin1 != false"),
                Exp.ne(Exp.boolBin("boolBin1"), Exp.val(false)));
    }

    // Logical expressions are always treated on boolean operands
    // this can also be an expression that evaluates to a boolean result
    @Test
    void binBooleanImplicitLogicalComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.boolBin1 and $.boolBin2"),
                Exp.and(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.boolBin1 or $.boolBin2"),
                Exp.or(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("not($.boolBin1)"),
                Exp.not(Exp.boolBin("boolBin1")));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("exclusive($.boolBin1, $.boolBin2)"),
                Exp.exclusive(Exp.boolBin("boolBin1"), Exp.boolBin("boolBin2")));
    }

    @Test
    void implicitDefaultIntComparison() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 < $.intBin2"),
                Exp.lt(Exp.intBin("intBin1"), Exp.intBin("intBin2")));
    }

    @Test
    void secondDegreeImplicitCastingFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples + $.bananas) > 10.5"),
                Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)));
    }

    @Test
    void secondDegreeComplicatedFloatFirstImplicitCastingFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples + $.bananas) > 10.5 " +
                        "and ($.oranges + $.grapes) <= 5"),
                Exp.and(
                        Exp.gt(Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")), Exp.val(10.5)),
                        Exp.le(Exp.add(Exp.intBin("oranges"), Exp.intBin("grapes")), Exp.val(5)))
        );
    }

    @Test
    void secondDegreeComplicatedIntFirstImplicitCastingFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.apples + $.bananas) > 5 " +
                        "and ($.oranges + $.grapes) <= 10.5"),
                Exp.and(
                        Exp.gt(Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.val(5)),
                        Exp.le(Exp.add(Exp.floatBin("oranges"), Exp.floatBin("grapes")), Exp.val(10.5)))
        );
    }

    @Test
    void thirdDegreeComplicatedDefaultInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(($.apples + $.bananas) + $.oranges) > 10"),
                Exp.gt(
                        Exp.add(Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")), Exp.intBin("oranges")),
                        Exp.val(10))
        );
    }

    @Test
    void thirdDegreeComplicatedImplicitCastingFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(($.apples + $.bananas) + $.oranges) > 10.5"),
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")),
                                Exp.floatBin("oranges")
                        ),
                        Exp.val(10.5))
        );
    }

    @Test
    void forthDegreeComplicatedDefaultInt() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(($.apples + $.bananas) + ($.oranges + $.acai)) > 10"),
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.intBin("apples"), Exp.intBin("bananas")),
                                Exp.add(Exp.intBin("oranges"), Exp.intBin("acai"))),
                        Exp.val(10))
        );
    }

    @Test
    void forthDegreeComplicatedImplicitCastingFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(($.apples + $.bananas) + ($.oranges + $.acai)) > 10.5"),
                Exp.gt(
                        Exp.add(
                                Exp.add(Exp.floatBin("apples"), Exp.floatBin("bananas")),
                                Exp.add(Exp.floatBin("oranges"), Exp.floatBin("acai"))),
                        Exp.val(10.5))
        );
    }

    @Test
    void complicatedWhenImplicitTypeInt() {
        Exp expected = Exp.eq(
                Exp.intBin("a"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(1)
                        ), Exp.intBin("a1"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(2)
                        ), Exp.intBin("a2"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(3)
                        ), Exp.intBin("a3"),
                        Exp.add(Exp.intBin("a4"), Exp.val(1))
                )
        );

        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.a == (when($.b == 1 => $.a1, $.b == 2 => $.a2, " +
                        "$.b == 3 => $.a3, default => $.a4 + 1))"), expected);
    }

    // TODO: FMWK-533 Implicit Type Detection for Control Structures
    //@Test
    void complicatedWhenImplicitTypeString() {
        Exp expected = Exp.eq(
                Exp.stringBin("a"),
                Exp.cond(
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(1)
                        ), Exp.stringBin("a1"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(2)
                        ), Exp.stringBin("a2"),
                        Exp.eq(
                                Exp.intBin("b"),
                                Exp.val(3)
                        ), Exp.stringBin("a3"),
                        Exp.val("hello")
                )
        );

        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.a == (when($.b == 1 => $.a1, $.b == 2 => $.a2, " +
                        "$.b == 3 => $.a3, default => \"hello\"))"), expected);
    }
}
