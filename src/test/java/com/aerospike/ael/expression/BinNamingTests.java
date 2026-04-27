package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.Expression;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BinNamingTests {

    @Nested
    class CharsetAndQuoting {

        @Test
        void binNameWithAt() {
            Exp expected = Exp.eq(Exp.intBin("name@host"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.name@host == 5"), expected);
        }

        @Test
        void binNameStartingWithAt() {
            Exp expected = Exp.eq(Exp.intBin("@attr"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.@attr == 5"), expected);
        }

        @Test
        void binNameEndingWithAt() {
            Exp expected = Exp.eq(Exp.intBin("name@"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.name@ == 5"), expected);
        }

        @Test
        void binNameSingleAt() {
            Exp expected = Exp.eq(Exp.intBin("@"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.@ == 5"), expected);
        }

        @Test
        void quotedBinNameDouble() {
            Exp expected = Exp.eq(Exp.intBin("my-bin"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.\"my-bin\" == 5"), expected);
        }

        @Test
        void quotedBinNameSingle() {
            Exp expected = Exp.eq(Exp.intBin("my-bin"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.'my-bin' == 5"), expected);
        }

        @Test
        void quotedBinNameDollar() {
            Exp expected = Exp.eq(Exp.intBin("$price"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.'$price' == 5"), expected);
        }

        @Test
        void quotedBinNameSpaces() {
            Exp expected = Exp.eq(Exp.intBin("has spaces"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.\"has spaces\" == 5"), expected);
        }

        @Test
        void quotedUnquotedEquivalence() {
            Expression exp1 = Exp.build(parseFilterExp(ExpressionContext.of("$.myBin == 5")));
            Expression exp2 = Exp.build(parseFilterExp(ExpressionContext.of("$.\"myBin\" == 5")));
            Expression exp3 = Exp.build(parseFilterExp(ExpressionContext.of("$.'myBin' == 5")));
            assertEquals(exp1, exp2);
            assertEquals(exp1, exp3);
        }

        @Test
        void quotedUnquotedAtEquiv() {
            Expression exp1 = Exp.build(parseFilterExp(ExpressionContext.of("$.name@host == 5")));
            Expression exp2 = Exp.build(parseFilterExp(ExpressionContext.of("$.\"name@host\" == 5")));
            assertEquals(exp1, exp2);
        }

        @Test
        void quotedUnquotedKeywordEquiv() {
            Expression exp1 = Exp.build(parseFilterExp(ExpressionContext.of("$.true == 5")));
            Expression exp2 = Exp.build(parseFilterExp(ExpressionContext.of("$.\"true\" == 5")));
            Expression exp3 = Exp.build(parseFilterExp(ExpressionContext.of("$.'true' == 5")));
            assertEquals(exp1, exp2);
            assertEquals(exp1, exp3);
        }

        @Test
        void quotedUnquotedInEquiv() {
            Expression exp1 = Exp.build(parseFilterExp(ExpressionContext.of("$.in == 5")));
            Expression exp2 = Exp.build(parseFilterExp(ExpressionContext.of("$.\"in\" == 5")));
            Expression exp3 = Exp.build(parseFilterExp(ExpressionContext.of("$.'in' == 5")));
            assertEquals(exp1, exp2);
            assertEquals(exp1, exp3);
        }

        @Test
        void negEmptyQuotedBinDouble() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.\"\" == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Bin name must not be empty");
        }

        @Test
        void negEmptyQuotedBinSingle() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.'' == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Bin name must not be empty");
        }

        @Test
        void negAtInMapKey() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.key@val == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void negAtInVariableDef() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("let(x@ = 5) then (${x} + 1)")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void negDigitOnlyBinName() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.123 == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void negAtInFunctionCall() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("my@func($.bin) == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void binNameDigitStartWithAt() {
            Exp expected = Exp.eq(Exp.intBin("123@"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.123@ == 5"), expected);
        }

        @Test
        void atBinWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("name@host")),
                    Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.name@host.key == 5"), expected);
        }

        @Test
        void atBinWithListIndex() {
            Exp expected = Exp.eq(
                    ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0), Exp.listBin("@attr")),
                    Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.@attr.[0] == 5"), expected);
        }
    }

    @Nested
    class KeywordCollision {

        @Test
        void binNamedTrue() {
            Exp expected = Exp.eq(Exp.intBin("true"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.true == 5"), expected);
        }

        @Test
        void binNamedFalse() {
            Exp expected = Exp.eq(Exp.intBin("false"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.false == 5"), expected);
        }

        @Test
        void binNamedAnd() {
            Exp expected = Exp.eq(Exp.intBin("and"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.and == 1"), expected);
        }

        @Test
        void binNamedOr() {
            Exp expected = Exp.eq(Exp.intBin("or"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.or == 1"), expected);
        }

        @Test
        void binNamedNot() {
            Exp expected = Exp.eq(Exp.intBin("not"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.not == 1"), expected);
        }

        @Test
        void binNamedLet() {
            Exp expected = Exp.eq(Exp.intBin("let"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.let == 1"), expected);
        }

        @Test
        void binNamedThen() {
            Exp expected = Exp.eq(Exp.intBin("then"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.then == 1"), expected);
        }

        @Test
        void binNamedWhen() {
            Exp expected = Exp.eq(Exp.intBin("when"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.when == 1"), expected);
        }

        @Test
        void binNamedDefault() {
            Exp expected = Exp.eq(Exp.intBin("default"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.default == 1"), expected);
        }

        @Test
        void binNamedExclusive() {
            Exp expected = Exp.eq(Exp.intBin("exclusive"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.exclusive == 1"), expected);
        }

        @Test
        void binNamedGet() {
            Exp expected = Exp.eq(Exp.intBin("get"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.get == 1"), expected);
        }

        @Test
        void binNamedType() {
            Exp expected = Exp.eq(Exp.intBin("type"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.type == 1"), expected);
        }

        @Test
        void binNamedReturn() {
            Exp expected = Exp.eq(Exp.intBin("return"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.return == 1"), expected);
        }

        @Test
        void binNamedRemove() {
            Exp expected = Exp.eq(Exp.intBin("remove"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.remove == 1"), expected);
        }

        @Test
        void binNamedInsert() {
            Exp expected = Exp.eq(Exp.intBin("insert"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.insert == 1"), expected);
        }

        @Test
        void binNamedSet() {
            Exp expected = Exp.eq(Exp.intBin("set"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.set == 1"), expected);
        }

        @Test
        void binNamedAppend() {
            Exp expected = Exp.eq(Exp.intBin("append"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.append == 1"), expected);
        }

        @Test
        void binNamedIncrement() {
            Exp expected = Exp.eq(Exp.intBin("increment"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.increment == 1"), expected);
        }

        @Test
        void binNamedClear() {
            Exp expected = Exp.eq(Exp.intBin("clear"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.clear == 1"), expected);
        }

        @Test
        void binNamedSort() {
            Exp expected = Exp.eq(Exp.intBin("sort"), Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.sort == 1"), expected);
        }

        @Test
        void keywordBinWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("true")),
                    Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.true.key == 5"), expected);
        }

        @Test
        void compoundAndWithKeywordBins() {
            Exp expected = Exp.and(
                    Exp.eq(Exp.intBin("and"), Exp.val(1)),
                    Exp.eq(Exp.intBin("or"), Exp.val(2)));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.and == 1 and $.or == 2"), expected);
        }

        @Test
        void compoundOrWithKeywordBins() {
            Exp expected = Exp.or(
                    Exp.eq(Exp.intBin("or"), Exp.val(1)),
                    Exp.eq(Exp.intBin("and"), Exp.val(2)));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.or == 1 or $.and == 2"), expected);
        }

        @Test
        void compoundNotWithKeywordBin() {
            Exp expected = Exp.and(
                    Exp.eq(Exp.intBin("not"), Exp.val(1)),
                    Exp.not(Exp.eq(Exp.intBin("x"), Exp.val(2))));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.not == 1 and not($.x == 2)"), expected);
        }

        @Test
        void keywordBinInWithInOp() {
            Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                    Exp.intBin("in"), Exp.val(List.of(1, 2, 3)));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.in in [1, 2, 3]"), expected);
        }

        @Test
        void binNamedInCasePreserved() {
            Exp expected = Exp.eq(Exp.intBin("IN"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.IN == 5"), expected);
        }
    }

    @Nested
    class KeywordCdtPaths {

        @Test
        void binWhenWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("when")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.when.key == 1"), expected);
        }

        @Test
        void binDefaultWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("default")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.default.key == 1"), expected);
        }

        @Test
        void binLetWithMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.mapBin("let")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.let.key == 1"), expected);
        }

        @Test
        void binAndWithListIndex() {
            Exp expected = Exp.eq(
                    ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0), Exp.listBin("and")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.and.[0] == 1"), expected);
        }

        @Test
        void binOrWithListIndex() {
            Exp expected = Exp.eq(
                    ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0), Exp.listBin("or")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.or.[0] == 1"), expected);
        }

        @Test
        void binThenWithListIndex() {
            Exp expected = Exp.eq(
                    ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0), Exp.listBin("then")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.then.[0] == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyWhen() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("when"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'when' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyDefault() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("default"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'default' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyAnd() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("and"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'and' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyOr() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("or"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'or' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyLet() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("let"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'let' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyThen() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("then"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'then' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyUnknown() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("unknown"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'unknown' == 1"), expected);
        }

        @Test
        void quotedKeywordMapKeyError() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("error"), Exp.mapBin("mapBin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin.'error' == 1"), expected);
        }
    }

    @Nested
    class UnquotedKeywordCdtRestriction {

        private static final String RESERVED_WORD_MSG = "reserved word";
        private static final String MUST_BE_QUOTED_MSG = "must be quoted";

        @ParameterizedTest
        @ValueSource(strings = {"when", "default", "and", "or", "let", "then",
                "unknown", "error", "true", "get"})
        void negUnquotedMapKey(String keyword) {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.mapBin.%s == 1".formatted(keyword))))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining(RESERVED_WORD_MSG)
                    .hasMessageContaining(MUST_BE_QUOTED_MSG);
        }

        @ParameterizedTest
        @ValueSource(strings = {"when", "unknown", "error"})
        void negUnquotedListValue(String keyword) {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.listBin.[=%s] == 1".formatted(keyword))))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining(RESERVED_WORD_MSG)
                    .hasMessageContaining(MUST_BE_QUOTED_MSG);
        }

        @ParameterizedTest
        @ValueSource(strings = {"unknown", "error"})
        void negUnquotedMapValue(String keyword) {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.mapBin.{=%s} == 1".formatted(keyword))))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining(RESERVED_WORD_MSG)
                    .hasMessageContaining(MUST_BE_QUOTED_MSG);
        }
    }

    @Nested
    class NullRestriction {

        @Test
        void negBinNameNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.null == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameNullUpper() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.NULL == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameNullMixed() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.nUlL == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameNullSubstring() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.nullify == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameEndsWithNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.mynull == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameCapitalNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.myNullVar == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameUnderscoredNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.my_null_bin == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negQuotedBinContainsNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.\"my-null-key\" == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negQuotedBinExactNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.\"null\" == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negQuotedBinExactNullSQ() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.'null' == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negQuotedBinNullUpper() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.'NULL_BIN' == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameNullWithAt() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.null@ == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negBinNameAtNull() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.@null == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void negQuotedBinNullAtHost() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.\"null@host\" == 5")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("null")
                    .hasMessageContaining("Bin name");
        }

        @Test
        void binNameNulAllowed() {
            Exp expected = Exp.eq(Exp.intBin("nul"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.nul == 5"), expected);
        }

        @Test
        void binNameNulCapAllowed() {
            Exp expected = Exp.eq(Exp.intBin("Nul"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.Nul == 5"), expected);
        }

        @Test
        void binNameNuLlAllowed() {
            Exp expected = Exp.eq(Exp.intBin("nu_ll"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.nu_ll == 5"), expected);
        }

        @Test
        void binNameLnulAllowed() {
            Exp expected = Exp.eq(Exp.intBin("lnul"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.lnul == 5"), expected);
        }

        @Test
        void binNameNuAtLlAllowed() {
            Exp expected = Exp.eq(Exp.intBin("nu@ll"), Exp.val(5));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.nu@ll == 5"), expected);
        }
    }
}
