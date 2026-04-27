package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.Value;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.Expression;
import com.aerospike.ael.client.exp.MapExp;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MapKeyTypingTests {

    @Nested
    class DigitOnlyPathKeys {

        @Test
        void digitOnlyMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(55L), Exp.mapBin("m")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.55 == 10"), expected);
        }

        @Test
        void largeDigitMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(12345678910L), Exp.mapBin("bin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.12345678910 == 1"), expected);
        }

        @Test
        void singleDigitMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(1L), Exp.mapBin("m")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.1 == 10"), expected);
        }

        @Test
        void zeroMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0L), Exp.mapBin("m")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.0 == 10"), expected);
        }

        @Test
        void leadingZeroMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(7L), Exp.mapBin("m")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.007 == 10"), expected);
        }

        @Test
        void quotedDigitMapKeyString() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                            Exp.val("55"), Exp.mapBin("m")),
                    Exp.val("val"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.\"55\" == \"val\""), expected);
        }

        @Test
        void quotedDigitMapKeySQString() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                            Exp.val("55"), Exp.mapBin("m")),
                    Exp.val("val"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.'55' == 'val'"), expected);
        }

        @Test
        void typeContrastDigitMapKey() {
            Expression intKey = Exp.build(parseFilterExp(ExpressionContext.of("$.m.55 == 10")));
            Expression strKey = Exp.build(parseFilterExp(ExpressionContext.of("$.m.\"55\" == 10")));
            assertNotEquals(intKey, strKey);
        }

        @Test
        void mixedDigitAndStringKeys() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("key"), Exp.bin("bin", Exp.Type.MAP),
                            CTX.mapKey(Value.get(55L))),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.55.key == 10"), expected);
        }

        @Test
        void digitKeyWithPathFunction() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(55L), Exp.mapBin("m")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.55.get(type: INT) == 10"), expected);
        }

        @Test
        void negDigitKeyOverflow() {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.bin.99999999999999999999 == 10")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("out of range");
        }
    }

    @Nested
    class SignedPathKeys {

        @Test
        void negativeSignedMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(-100L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.-100 == 10"), expected);
        }

        @Test
        void positiveSignedMapKey() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(100L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.+100 == 10"), expected);
        }

        @Test
        void combinedSignedMapKeys() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(100L), Exp.bin("bin", Exp.Type.MAP),
                            CTX.mapKey(Value.get(100L)), CTX.mapKey(Value.get(-100L))),
                    Exp.val(1));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.100.-100.+100 == 1"), expected);
        }

        @Test
        void signedZeroMinus() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.-0 == 10"), expected);
        }

        @Test
        void signedZeroPlus() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(0L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.+0 == 10"), expected);
        }

        @Test
        void negSignedMapKeySpaceMinus() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.- 100 == 10")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void negSignedMapKeySpacePlus() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.+ 100 == 10")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void quotedSignedDigitIsString() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val("-100"), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.\"-100\" == 10"), expected);
        }

        @Test
        void signedKeyWithPathFunction() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(-100L), Exp.mapBin("m")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.m.-100.get(type: INT) == 10"), expected);
        }

        @Test
        void negDoubleMinusSign() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.--100 == 10")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void negPlusMinusMixedSign() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.+-100 == 10")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }
    }

    @Nested
    class HexBinaryPathKeys {

        @Test
        void hexMapKeyIsLong() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(255L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.0xff == 10"), expected);
        }

        @Test
        void hexMapKeyCaseProducesSameLong() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(255L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.0xFF == 10"), expected);
        }

        @Test
        void hexMapKeyUpperPrefix() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(255L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.0Xff == 10"), expected);
        }

        @Test
        void binaryMapKeyIsLong() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(10L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.0b1010 == 10"), expected);
        }

        @Test
        void binaryMapKeyUpperPrefix() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(10L), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.0B1010 == 10"), expected);
        }

        @Test
        void fullMixedPath() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(10L), Exp.bin("bin", Exp.Type.MAP),
                            CTX.mapKey(Value.get(100L)), CTX.mapKey(Value.get(-100L)),
                            CTX.mapKey(Value.get(100L)), CTX.mapKey(Value.get(255L))),
                    Exp.val(1));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.bin.100.-100.+100.0xff.0b1010 == 1"), expected);
        }

        @Test
        void quotedHexNotEquivalent() {
            Expression unquoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.0xff == 10")));
            Expression quoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.\"0xff\" == 10")));
            assertNotEquals(unquoted, quoted);
        }

        @Test
        void quotedBinaryNotEquivalent() {
            Expression unquoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.0b1010 == 10")));
            Expression quoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.\"0b1010\" == 10")));
            assertNotEquals(unquoted, quoted);
        }

        @Test
        void hexMapKeyMaxLong() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(Long.MAX_VALUE), Exp.mapBin("bin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.bin.0x7FFFFFFFFFFFFFFF == 1"), expected);
        }

        @Test
        void hexMapKeyUnsignedLongBoundary() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(Long.MIN_VALUE), Exp.mapBin("bin")),
                    Exp.val(1));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.bin.0x8000000000000000 == 1"), expected);
        }

        @Test
        void hexMapKeyOverflow() {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.bin.0x10000000000000000 == 1")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("out of range");
        }

        @Test
        void hexKeyWithPathFunction() {
            Exp expected = Exp.eq(
                    MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                            Exp.val(255L), Exp.mapBin("m")),
                    Exp.val("x"));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.m.0xff.get(type: STRING) == \"x\""), expected);
        }
    }

    @Nested
    class BraceContextKeys {

        @Test
        void intKeyInKeyRange() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(55L), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{55-z}"), expected);
        }

        @Test
        void intKeyInKeyList() {
            Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                    Exp.val(List.of(55L, "abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{55,abc}"), expected);
        }

        @Test
        void quotedDigitKeyInKeyList() {
            Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                    Exp.val(List.of("55", "abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{\"55\",abc}"), expected);
        }

        @Test
        void intKeyRangeBothEndpoints() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(10L), Exp.val(200L), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{10-200}"), expected);
        }

        @Test
        void quotedDigitKeyInKeyRange() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val("55"), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{\"55\"-z}"), expected);
        }

        @Test
        void hexIntInKeyRange() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(255L), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{0xff-z}"), expected);
        }

        @Test
        void hexIntInKeyList() {
            Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                    Exp.val(List.of(255L, "abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{0xff,abc}"), expected);
        }

        @Test
        void binaryIntInKeyList() {
            Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                    Exp.val(List.of(10L, "abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{0b1010,abc}"), expected);
        }

        @Test
        void binaryIntInKeyRange() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(10L), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{0b1010-z}"), expected);
        }

        @Test
        void intKeyInRelativeIndex() {
            Exp expected = MapExp.getByKeyRelativeIndexRange(MapReturnType.VALUE,
                    Exp.val(55L), Exp.val(0), Exp.val(1), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{0:1~55}"), expected);
        }

        @Test
        void singleIntBracesIsMapIndex() {
            Exp expected = Exp.eq(
                    MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.INT,
                            Exp.val(55), Exp.mapBin("bin")),
                    Exp.val(10));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{55} == 10"), expected);
        }

        @Test
        void disambiguationIndexVsKeyList() {
            Expression mapIndex = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.{55} == 10")));
            Expression keyList = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.{55,abc}")));
            assertNotEquals(mapIndex, keyList);
        }

        @Test
        void singleAbcKeyListRegression() {
            Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                    Exp.val(List.of("abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{abc}"), expected);
        }

        @Test
        void intKeyInvertedRange() {
            Exp expected = MapExp.getByKeyRange(
                    MapReturnType.VALUE | MapReturnType.INVERTED,
                    Exp.val(55L), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{!55-z}"), expected);
        }

        @Test
        void intKeyInvertedList() {
            Exp expected = MapExp.getByKeyList(
                    MapReturnType.VALUE | MapReturnType.INVERTED,
                    Exp.val(List.of(55L, "abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{!55,abc}"), expected);
        }

        @Test
        void negNegativeIntInBraces() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.{-55,abc}")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void negNegativeIntInBraceRange() {
            assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.bin.{-55-z}")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("at character");
        }

        @Test
        void quotedNegIntInBraces() {
            Exp expected = MapExp.getByKeyList(MapReturnType.VALUE,
                    Exp.val(List.of("-55", "abc")), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(ExpressionContext.of("$.bin.{\"-55\",abc}"), expected);
        }

        @Test
        void negIntKeyOverflowInBraces() {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.bin.{99999999999999999999-z}")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("out of range");
        }

        @Test
        void hexKeyMaxLongInBraces() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(Long.MAX_VALUE), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.bin.{0x7FFFFFFFFFFFFFFF-z}"), expected);
        }

        @Test
        void hexKeyUnsignedBoundaryInBraces() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(Long.MIN_VALUE), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.bin.{0x8000000000000000-z}"), expected);
        }

        @Test
        void hexKeyOverflowInBraces() {
            assertThatThrownBy(() -> parseFilterExp(
                    ExpressionContext.of("$.bin.{0x10000000000000000-z}")))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("out of range");
        }

        @Test
        void decimalKeyUnsignedBoundaryInBraces() {
            Exp expected = MapExp.getByKeyRange(MapReturnType.VALUE,
                    Exp.val(Long.MIN_VALUE), Exp.val("z"), Exp.mapBin("bin"));
            parseFilterExpressionAndCompare(
                    ExpressionContext.of("$.bin.{9223372036854775808-z}"), expected);
        }
    }

    @Nested
    class MapKeyEquivalence {

        @Test
        void quotedUnquotedMapKeyEquiv() {
            Expression unquoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.key == 10")));
            Expression doubleQuoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.\"key\" == 10")));
            Expression singleQuoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.'key' == 10")));
            assertEquals(unquoted, doubleQuoted);
            assertEquals(unquoted, singleQuoted);
        }

        @Test
        void mapKeyInQuotedEquiv() {
            Expression unquoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.in == 10")));
            Expression doubleQuoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.\"in\" == 10")));
            Expression singleQuoted = Exp.build(parseFilterExp(ExpressionContext.of("$.bin.'in' == 10")));
            assertEquals(unquoted, doubleQuoted);
            assertEquals(unquoted, singleQuoted);
        }
    }
}
