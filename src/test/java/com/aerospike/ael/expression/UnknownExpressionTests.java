package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnknownExpressionTests {

    /**
     * Tests for {@code unknown} and {@code error} keywords as expressions.
     * Outside a {@code when} branch, these will most likely cause a server-side exception on evaluation.
     */
    @Nested
    class KeywordUsage {

        @Test
        void unknownAsWhenDefault() {
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("a"), Exp.val(5)),
                    Exp.intBin("a"),
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.a > 5 => $.a, default => unknown)"), expected);
        }

        @Test
        void errorAsWhenDefault() {
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("a"), Exp.val(5)),
                    Exp.intBin("a"),
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.a > 5 => $.a, default => error)"), expected);
        }

        @Test
        void unknownAsWhenActionBranch() {
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("a"), Exp.val(5)),
                    Exp.unknown(),
                    Exp.intBin("a")
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.a > 5 => unknown, default => $.a)"), expected);
        }

        @Test
        void unknownInMultipleBranches() {
            Exp expected = Exp.cond(
                    Exp.eq(Exp.intBin("x"), Exp.val(1)), Exp.val(10),
                    Exp.eq(Exp.intBin("x"), Exp.val(2)), Exp.unknown(),
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.x == 1 => 10, $.x == 2 => unknown, default => error)"),
                    expected);
        }

        @Test
        void unknownInNestedWhen() {
            Exp inner = Exp.cond(
                    Exp.gt(Exp.intBin("b"), Exp.val(0)), Exp.intBin("b"),
                    Exp.unknown()
            );
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("a"), Exp.val(0)), inner,
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.a > 0 => when($.b > 0 => $.b, default => unknown), "
                            + "default => unknown)"),
                    expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownAsStandaloneExpression() {
            Exp expected = Exp.unknown();
            parseFilterExpressionAndCompare(ExpressionContext.of("unknown"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void errorAsStandaloneExpression() {
            Exp expected = Exp.unknown();
            parseFilterExpressionAndCompare(ExpressionContext.of("error"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownInArithmetic() {
            Exp expected = Exp.add(Exp.unknown(), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("unknown + 1"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void errorInArithmetic() {
            Exp expected = Exp.add(Exp.unknown(), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("error + 1"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownInComparison() {
            Exp expected = Exp.eq(Exp.unknown(), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("unknown == 5"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void binComparedToUnknown() {
            Exp expected = Exp.eq(Exp.intBin("a"), Exp.unknown());
            parseFilterExpressionAndCompare(ExpressionContext.of("$.a == unknown"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownComparedToBin() {
            Exp expected = Exp.eq(Exp.unknown(), Exp.intBin("a"));
            parseFilterExpressionAndCompare(ExpressionContext.of("unknown == $.a"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void binGreaterThanError() {
            Exp expected = Exp.gt(Exp.intBin("a"), Exp.unknown());
            parseFilterExpressionAndCompare(ExpressionContext.of("$.a > error"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void binPlusUnknown() {
            Exp expected = Exp.add(Exp.intBin("a"), Exp.unknown());
            parseFilterExpressionAndCompare(ExpressionContext.of("$.a + unknown"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownInLogicalAnd() {
            Exp expected = Exp.and(Exp.unknown(), Exp.gt(Exp.intBin("a"), Exp.val(1)));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("unknown and $.a > 1"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownInLogicalOr() {
            Exp expected = Exp.or(Exp.gt(Exp.intBin("a"), Exp.val(1)), Exp.unknown());
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.a > 1 or unknown"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void notUnknown() {
            Exp expected = Exp.not(Exp.unknown());
            parseFilterExpressionAndCompare(ExpressionContext.of("not(unknown)"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownInExclusive() {
            Exp expected = Exp.exclusive(Exp.unknown(), Exp.gt(Exp.intBin("a"), Exp.val(1)));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("exclusive(unknown, $.a > 1)"), expected);
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownInLetDef() {
            Exp expected = Exp.let(
                    Exp.def("x", Exp.unknown()),
                    Exp.add(Exp.var("x"), Exp.val(1))
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("let(x = unknown) then (${x} + 1)"), expected);
        }

        @Test
        void errorAndUnknownAreEquivalent() {
            parseAelAndCompare("unknown", "error");
        }

        @Test
        void equivalenceInWhenContext() {
            parseAelAndCompare(
                    "when($.a > 0 => $.a, default => unknown)",
                    "when($.a > 0 => $.a, default => error)");
        }

        @Test
        void spacingAroundArrowWithUnknown() {
            String canonical = "when($.a > 5 => $.a, default => unknown)";
            parseAelAndCompare("when($.a>5=>$.a,default=>unknown)", canonical);
            parseAelAndCompare("when( $.a > 5 => $.a , default => unknown )", canonical);
        }

        @Test
        void spacingAroundArrowWithError() {
            String canonical = "when($.a > 5 => $.a, default => error)";
            parseAelAndCompare("when($.a>5=>$.a,default=>error)", canonical);
            parseAelAndCompare("when( $.a > 5 => $.a , default => error )", canonical);
        }
    }

    @Nested
    class BinNameVariations {

        @Test
        void binNamedUnknownUnquoted() {
            Exp expected = Exp.eq(Exp.intBin("unknown"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.unknown == 1"), expected);
        }

        @Test
        void binNamedUnknownSingleQuoted() {
            Exp expected = Exp.eq(Exp.intBin("unknown"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.'unknown' == 1"), expected);
        }

        @Test
        void binNamedUnknownDoubleQuoted() {
            Exp expected = Exp.eq(Exp.intBin("unknown"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.\"unknown\" == 1"), expected);
        }

        @Test
        void binNamedUnknownQuotingEquiv() {
            parseAelAndCompare("$.unknown == 1", "$.'unknown' == 1");
            parseAelAndCompare("$.unknown == 1", "$.\"unknown\" == 1");
        }

        @Test
        void binNamedErrorUnquoted() {
            Exp expected = Exp.eq(Exp.intBin("error"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.error == 1"), expected);
        }

        @Test
        void binNamedErrorSingleQuoted() {
            Exp expected = Exp.eq(Exp.intBin("error"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.'error' == 1"), expected);
        }

        @Test
        void binNamedErrorDoubleQuoted() {
            Exp expected = Exp.eq(Exp.intBin("error"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.\"error\" == 1"), expected);
        }

        @Test
        void binNamedErrorQuotingEquiv() {
            parseAelAndCompare("$.error == 1", "$.'error' == 1");
            parseAelAndCompare("$.error == 1", "$.\"error\" == 1");
        }

        @Test
        void binNameContainingUnknownPrefix() {
            Exp expected = Exp.eq(Exp.intBin("unknown_flag"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.unknown_flag == 1"), expected);
        }

        @Test
        void binNameContainingErrorPrefix() {
            Exp expected = Exp.eq(Exp.intBin("error_code"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.error_code == 1"), expected);
        }

        @Test
        void binNameContainingUnknownSuffix() {
            Exp expected = Exp.eq(Exp.intBin("is_unknown"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.is_unknown == 1"), expected);
        }

        @Test
        void binNameContainingErrorSuffix() {
            Exp expected = Exp.eq(Exp.intBin("on_error"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.on_error == 1"), expected);
        }
    }

    @Nested
    class CdtPaths {

        @Test
        void binUnknownWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("unknown")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.unknown.key == 1"), expected);
        }

        @Test
        void binErrorWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("error")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.error.key == 1"), expected);
        }

        @Test
        void binUnknownWithListIndex() {
            Exp expected = Exp.eq(
                    ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0), Exp.listBin("unknown")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.unknown.[0] == 1"), expected);
        }

        @Test
        void binErrorWithListIndex() {
            Exp expected = Exp.eq(
                    ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0), Exp.listBin("error")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.error.[0] == 1"), expected);
        }

        @Test
        void binUnknownWithMapWildcardCount() {
            Exp expected = Exp.gt(MapExp.size(Exp.mapBin("unknown")), Exp.val(0));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.unknown.{}.count() > 0"), expected);
        }
    }

    @Nested
    class Coexistence {

        @Test
        void unknownBinAndKeywordInSameExpr() {
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("unknown"), Exp.val(5)),
                    Exp.intBin("unknown"),
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.unknown > 5 => $.unknown, default => unknown)"),
                    expected);
        }

        @Test
        void errorBinAndKeywordInSameExpr() {
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("error"), Exp.val(5)),
                    Exp.intBin("error"),
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.error > 5 => $.error, default => error)"),
                    expected);
        }
    }

    @Nested
    class Negative {

        @Test
        void unknownWithParentheses() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("unknown($.a) == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("mismatched input");
        }

        @Test
        void errorWithParentheses() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("error($.a) == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("mismatched input");
        }

        @Test
        void uppercaseUnknownIsNotKeyword() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("UNKNOWN == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("no viable alternative");
        }

        @Test
        void mixedCaseErrorIsNotKeyword() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("Error == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("no viable alternative");
        }

        @Test
        void unknownAsInRightOperand() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.a in unknown")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("IN operation requires a List");
        }

        @Test
        void errorAsInRightOperand() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.a in error")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("IN operation requires a List");
        }

        @Test
        void letUnknownVarUsedInIn() {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("let(x = unknown) then ($.a.get(type: INT) in ${x})")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("non-List type");
        }

        // Parses correctly but will most likely cause a server-side exception on evaluation
        @Test
        void unknownAsInLeftOperand() {
            Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                    Exp.unknown(), Exp.listBin("list"));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("unknown in $.list"), expected);
        }
    }

    @Nested
    class PlaceholderIntegration {

        @Test
        void unknownWithPlaceholderInWhen() {
            Exp expected = Exp.cond(
                    Exp.gt(Exp.intBin("a"), Exp.val(10)),
                    Exp.intBin("a"),
                    Exp.unknown()
            );
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("when($.a > ?0 => $.a, default => unknown)",
                            PlaceholderValues.of(10)),
                    expected);
        }
    }
}
