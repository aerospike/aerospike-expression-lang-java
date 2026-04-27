package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.Expression;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.fluent.AerospikeComparator;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BlobTests {

    // ---- Hex BLOB Literal Parsing ----

    @Test
    void blobHexUpperCase() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == X'ff00'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{(byte) 0xff, 0x00})));
    }

    @Test
    void blobHexLowerCase() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == x'ff00'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{(byte) 0xff, 0x00})));
    }

    @Test
    void blobHexMixedCase() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == X'aAbBcCdDeEfF'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{
                        (byte) 0xaa, (byte) 0xbb, (byte) 0xcc,
                        (byte) 0xdd, (byte) 0xee, (byte) 0xff})));
    }

    @Test
    void blobHexEmpty() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == X''"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[0])));
    }

    @Test
    void blobHexLong() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == x'102030405060708090abcdef'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{
                        0x10, 0x20, 0x30, 0x40, 0x50, 0x60,
                        0x70, (byte) 0x80, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef})));
    }

    // ---- Base64 BLOB Literal Parsing ----

    @Test
    void b64UpperCase() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == B64'AQID'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{1, 2, 3})));
    }

    @Test
    void b64LowerCase() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == b64'AQID'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{1, 2, 3})));
    }

    @Test
    void b64WithPadding() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == b64'AQ=='"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{1})));
    }

    @Test
    void b64Empty() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == b64''"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[0])));
    }

    // ---- Hex and Base64 Equivalence ----

    @Test
    void hexAndB64Equivalence() {
        Expression hexExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.b.get(type: BLOB) == X'010203'")));
        Expression b64Exp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.b.get(type: BLOB) == b64'AQID'")));
        assertThat(hexExp).isEqualTo(b64Exp);
    }

    // ---- Negative Parsing ----

    @Test
    void negativeOddHexLength() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'f' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    @Test
    void negativeOddThreeHexChars() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'abc' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    @Test
    void negativeNonHexChars() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ZZZZ' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeHexWhitespace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff 00' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeInvalidB64Chars() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("b64'!!!!' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeB64Whitespace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("b64'AQ ID' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input");
    }

    @Test
    void negativeInvalidB64Content() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("b64'A' == $.b.get(type: BLOB)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Base64 BLOB literal contains invalid Base64 content");
    }

    // ---- BLOB Bin Comparison ----

    @Test
    void blobHexReversed() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("X'ff00' == $.b.get(type: BLOB)"),
                Exp.eq(Exp.val(new byte[]{(byte) 0xff, 0x00}), Exp.blobBin("b")));
    }

    @Test
    void blobHexInequality() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) != X'ff00'"),
                Exp.ne(Exp.blobBin("b"), Exp.val(new byte[]{(byte) 0xff, 0x00})));
    }

    @Test
    void b64BinComparison() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == b64'AQID'"),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{1, 2, 3})));
    }

    @Test
    void stringBlobBackwardCompat() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) == \"AQID\""),
                Exp.eq(Exp.blobBin("b"), Exp.val(new byte[]{1, 2, 3})));
    }

    @Test
    void blobOrderingGt() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) > X'ff00'"),
                Exp.gt(Exp.blobBin("b"), Exp.val(new byte[]{(byte) 0xff, 0x00})));
    }

    // ---- CDT Path Comparison ----

    @Test
    void cdtPathHexComparison() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE, Exp.Type.BLOB,
                        Exp.val(0), Exp.listBin("list")),
                Exp.val(new byte[]{1, 2, 3}));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == x'010203'"), expected);
    }

    @Test
    void cdtPathB64Comparison() {
        Expression hexExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == x'010203'")));
        Expression b64Exp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == b64'AQID'")));
        assertThat(hexExp).isEqualTo(b64Exp);
    }

    @Test
    void cdtPathStringBackCompat() {
        Expression hexExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == x'010203'")));
        Expression stringExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == \"AQID\"")));
        assertThat(hexExp).isEqualTo(stringExp);
    }

    @Test
    void cdtPathAllThreeEqual() {
        Expression hexExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == x'010203'")));
        Expression b64Exp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == b64'AQID'")));
        Expression strExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.list.[0].get(type: BLOB) == \"AQID\"")));
        assertThat(hexExp).isEqualTo(b64Exp).isEqualTo(strExp);
    }

    @Test
    void cdtPathStringReversed() {
        Exp expected = Exp.eq(
                Exp.val(new byte[]{1, 2, 3}),
                ListExp.getByIndex(
                        ListReturnType.VALUE, Exp.Type.BLOB,
                        Exp.val(0), Exp.listBin("list")));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("\"AQID\" == $.list.[0].get(type: BLOB)"), expected);
    }

    // ---- Comparison Edge Cases ----

    @Test
    void negativeBlobVsString() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.blobBin.get(type: BLOB) == $.strBin.get(type: STRING)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare BLOB to STRING");
    }

    @Test
    void stringAsBase64NotHex() {
        Expression strExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.b.get(type: BLOB) == \"ff00\"")));
        Expression hexExp = Exp.build(parseFilterExp(
                ExpressionContext.of("$.b.get(type: BLOB) == X'ff00'")));
        assertThat(strExp).isNotEqualTo(hexExp);
    }

    @Test
    void negativeInvalidBase64StringVsBlob() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.b.get(type: BLOB) == \"not-base64!!!\"")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("not valid Base64");
    }

    @Test
    void getTypeBlobTwoBins() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b1.get(type: BLOB) == $.b2.get(type: BLOB)"),
                Exp.eq(Exp.blobBin("b1"), Exp.blobBin("b2")));
    }

    @Test
    void negativeBlobVsInt() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.b.get(type: BLOB) == 42")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare BLOB to INT");
    }

    // ---- BLOB in List Constants ----

    @Test
    void listWithHexBlob() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.lb.get(type: LIST) == [X'ff00']"),
                Exp.eq(Exp.listBin("lb"), Exp.val(List.of(new byte[]{(byte) 0xff, 0x00}))));
    }

    @Test
    void listWithBlobAndOtherTypes() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.lb.get(type: LIST) == [X'ff00', 42, \"hello\"]"),
                Exp.eq(Exp.listBin("lb"), Exp.val(List.of(
                        new byte[]{(byte) 0xff, 0x00}, 42L, "hello"))));
    }

    @Test
    void listWithMultipleBlobs() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.lb.get(type: LIST) == [X'aabb', X'ccdd']"),
                Exp.eq(Exp.listBin("lb"), Exp.val(List.of(
                        new byte[]{(byte) 0xaa, (byte) 0xbb},
                        new byte[]{(byte) 0xcc, (byte) 0xdd}))));
    }

    @Test
    void listWithB64Blob() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.lb.get(type: LIST) == [b64'AQID']"),
                Exp.eq(Exp.listBin("lb"), Exp.val(List.of(new byte[]{1, 2, 3}))));
    }

    // ---- BLOB in IN Expressions ----

    @Test
    void blobBinInBlobList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.blobBin("b"),
                Exp.val(List.of(new byte[]{(byte) 0xaa}, new byte[]{(byte) 0xbb})));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.b.get(type: BLOB) IN [X'aa', X'bb']"), expected);
    }

    @Test
    void blobLiteralInBlobList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(new byte[]{(byte) 0xaa}),
                Exp.val(List.of(
                        new byte[]{(byte) 0xaa},
                        new byte[]{(byte) 0xbb},
                        new byte[]{(byte) 0xcc})));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("X'aa' IN [X'aa', X'bb', X'cc']"), expected);
    }

    @Test
    void negativeHeterogeneousInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'aa' IN [X'aa', 1]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    // ---- BLOB as Map Key ----

    @Test
    void mapWithBlobKey() {
        SortedMap<Object, Object> expected = new TreeMap<>(new AerospikeComparator());
        expected.put(new byte[]{(byte) 0xff}, 42L);
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.get(type: MAP) == {X'ff': 42}"),
                Exp.eq(Exp.mapBin("mb"), Exp.val(expected)));
    }

    @Test
    void mapWithMultipleBlobKeys() {
        SortedMap<Object, Object> expected = new TreeMap<>(new AerospikeComparator());
        expected.put(new byte[]{(byte) 0xaa}, 1L);
        expected.put(new byte[]{(byte) 0xbb}, 2L);
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.get(type: MAP) == {X'aa': 1, X'bb': 2}"),
                Exp.eq(Exp.mapBin("mb"), Exp.val(expected)));
    }

    @Test
    void mapWithMixedKeyTypes() {
        SortedMap<Object, Object> expected = new TreeMap<>(new AerospikeComparator());
        expected.put(3L, 4L);
        expected.put("key", 2L);
        expected.put(new byte[]{(byte) 0xff}, 1L);
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.get(type: MAP) == {X'ff': 1, \"key\": 2, 3: 4}"),
                Exp.eq(Exp.mapBin("mb"), Exp.val(expected)));
    }

    @Test
    void mapWithB64BlobKey() {
        SortedMap<Object, Object> expected = new TreeMap<>(new AerospikeComparator());
        expected.put(new byte[]{1, 2, 3}, 42L);
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.get(type: MAP) == {b64'AQID': 42}"),
                Exp.eq(Exp.mapBin("mb"), Exp.val(expected)));
    }

    // ---- BLOB as Map Value ----

    @Test
    void mapWithBlobValue() {
        SortedMap<Object, Object> expected = new TreeMap<>(new AerospikeComparator());
        expected.put("key", new byte[]{(byte) 0xff, 0x00});
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.get(type: MAP) == {\"key\": X'ff00'}"),
                Exp.eq(Exp.mapBin("mb"), Exp.val(expected)));
    }

    @Test
    void mapWithBlobKeyAndValue() {
        SortedMap<Object, Object> expected = new TreeMap<>(new AerospikeComparator());
        expected.put(new byte[]{(byte) 0xaa}, new byte[]{(byte) 0xbb});
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mb.get(type: MAP) == {X'aa': X'bb'}"),
                Exp.eq(Exp.mapBin("mb"), Exp.val(expected)));
    }

    // ---- BLOB in CDT Value Selectors ----

    @Test
    void listValueSelectorWithBlob() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.lb.[=X'ff00'].get(type: BLOB) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void mapValueSelectorWithBlob() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.{=X'ff00'}.get(type: BLOB) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void listValueListWithBlob() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.lb.[=X'aa',X'bb'].get(type: BLOB) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void mapValueListWithBlob() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.{=X'aa',X'bb'}.get(type: BLOB) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void listRelativeRankWithBlob() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.lb.[#0:~X'ff00'].get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void mapRelativeRankWithBlob() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.{#0:~X'ff00'}.get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void b64CdtValueSelector() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.lb.[=b64'AQID'].get(type: BLOB) == 1")));
        assertThat(actual).isNotNull();
    }

    // ---- BLOB as CDT Path Map Key ----

    @Test
    void blobHexMapKeyAccess() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.X'ff'.get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void blobB64MapKeyAccess() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.b64'AQID'.get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void blobKeyRange() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.{X'aa'-X'ff'}.get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void blobKeyList() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.{X'aa',X'bb'}.get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    @Test
    void blobIndexRangeRelative() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("$.mb.{0:~X'ff'}.get(type: INT) == 1")));
        assertThat(actual).isNotNull();
    }

    // ---- Negative: Malformed BLOB as Map Key ----

    @Test
    void negBlobMapKeyOddHex() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.X'f'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    @Test
    void negBlobMapKeyOddHex3Chars() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.X'abc'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    @Test
    void negBlobMapKeyHexWithSpaces() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.X'ff 00'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse");
    }

    @Test
    void negBlobMapKeyInvalidHexChars() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.X'ZZZZ'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse");
    }

    @Test
    void negBlobMapKeyInvalidB64Content() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.b64'A'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Base64 BLOB literal contains invalid Base64 content");
    }

    @Test
    void negBlobMapKeyInvalidB64Chars() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.b64'!!!!'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse");
    }

    @Test
    void negBlobMapKeyB64WithSpaces() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.b64'AQ ID'.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse");
    }

    @Test
    void negBlobKeyRangeOddHex() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.{X'f'-X'ff'}.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    @Test
    void negBlobKeyRangeInvalidB64() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.{b64'A'-b64'AQID'}.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Base64 BLOB literal contains invalid Base64 content");
    }

    @Test
    void negBlobKeyListOddHex() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.{X'f',X'ff'}.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    @Test
    void negBlobRelativeKeyOddHex() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.mb.{0:~X'f'}.get(type: INT) == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("even number of hex characters");
    }

    // ---- BLOB Not Supported in Arithmetic ----

    @Test
    void negativeBlobIntArithmetic() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff' + 42 == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("BLOB type does not support arithmetic operations");
    }

    @Test
    void negativeSameTypeBlobArith() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff' + X'ee' == X'dd'")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("BLOB type does not support arithmetic operations");
    }

    @Test
    void negativeBlobMultiplicative() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff' * 2 == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("BLOB type does not support arithmetic operations");
    }

    @Test
    void negativeBlobModulo() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff' % 2 == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("BLOB type does not support arithmetic operations");
    }

    @Test
    void negativeBlobPower() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff' ** 2 == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("BLOB type does not support arithmetic operations");
    }

    @Test
    void negativeUnaryMinusBlob() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("-X'ff' == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("BLOB type does not support arithmetic operations");
    }

    @Test
    void negativeMixedTypeBitwise() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("X'ff' & 42 == 1")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare BLOB to INT");
    }

    // Not fully supported yet
    @Test
    void bitwiseSameTypeBlobOk() {
        Expression actual = Exp.build(parseFilterExp(
                ExpressionContext.of("(X'ff' & X'0f') == 15")));
        assertThat(actual).isNotNull();
    }
}
