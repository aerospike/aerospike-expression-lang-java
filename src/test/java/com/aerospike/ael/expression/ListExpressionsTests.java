package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListExpressionsTests {

    @Test
    void listByIndexInteger() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0] == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void listByIndexOtherTypes() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val("stringVal"));
        // Implicit detect as string
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0] == \"stringVal\""), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(type: STRING) == \"stringVal\""),
                expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(type: STRING, return: VALUE)" +
                " == \"stringVal\""), expected);

        expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.BOOL,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(true));
        // Implicit detect as boolean
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0] == true"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(type: BOOL) == true"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(type: BOOL, return: VALUE) == true"),
                expected);
    }

    @Test
    void listByValue() {
        Exp expected = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(100),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100] == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100].get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100].get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void listByValueHexAndBinarySelectors() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=0xff] == 100"),
                Exp.eq(ListExp.getByValue(ListReturnType.VALUE, Exp.val(255), Exp.listBin("listBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=0b10] == 100"),
                Exp.eq(ListExp.getByValue(ListReturnType.VALUE, Exp.val(2), Exp.listBin("listBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=-0xff] == 100"),
                Exp.eq(ListExp.getByValue(ListReturnType.VALUE, Exp.val(-255), Exp.listBin("listBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=-0b10] == 100"),
                Exp.eq(ListExp.getByValue(ListReturnType.VALUE, Exp.val(-2), Exp.listBin("listBin1")), Exp.val(100)));
    }

    @Test
    void listByValueIntegerMinDecimal() {
        Exp expected = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(Integer.MIN_VALUE),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=-2147483648] == 100"), expected);
    }

    @Test
    void listByValueCount() {
        Exp expected = Exp.gt(
                ListExp.getByValue(ListReturnType.COUNT,
                        Exp.val(100),
                        Exp.listBin("listBin1")),
                Exp.val(0)
        );
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100].count() > 0"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=100].[].count() > 0"), expected);
    }

    @Test
    void listByRank() {
        Exp expected = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-1] == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-1].get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-1].get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void listByRankHexAndBinarySelectors() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#0xff] == 100"),
                Exp.eq(ListExp.getByRank(ListReturnType.VALUE, Exp.Type.INT, Exp.val(255), Exp.listBin("listBin1")),
                        Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#0b10] == 100"),
                Exp.eq(ListExp.getByRank(ListReturnType.VALUE, Exp.Type.INT, Exp.val(2), Exp.listBin("listBin1")),
                        Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-0xff] == 100"),
                Exp.eq(ListExp.getByRank(ListReturnType.VALUE, Exp.Type.INT, Exp.val(-255), Exp.listBin("listBin1")),
                        Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-0b10] == 100"),
                Exp.eq(ListExp.getByRank(ListReturnType.VALUE, Exp.Type.INT, Exp.val(-2), Exp.listBin("listBin1")),
                        Exp.val(100)));
    }

    @Test
    void listIndexHexAndBinary() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0xff] == 100"),
                Exp.eq(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(255),
                        Exp.listBin("listBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[-0b10] == 100"),
                Exp.eq(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(-2),
                        Exp.listBin("listBin1")), Exp.val(100)));
    }

    @Test
    void listIndexRangeHex() {
        Exp expected = ListExp.getByIndexRange(
                ListReturnType.VALUE, Exp.val(1), Exp.val(2), Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin1.[0x01:0x03]"), expected);
    }

    @Test
    void listRankRangeHex() {
        Exp expected = ListExp.getByRankRange(
                ListReturnType.VALUE, Exp.val(1), Exp.val(2), Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin1.[#0x01:0x03]"), expected);
    }

    @Test
    void listValueRangeHex() {
        Exp expected = ListExp.getByValueRange(
                ListReturnType.VALUE, Exp.val(255), Exp.val(512), Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin1.[=0xff:0x200]"), expected);
    }

    @Test
    void listByRankIntegerMinDecimal() {
        Exp expected = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(Integer.MIN_VALUE),
                        Exp.listBin("listBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-2147483648] == 100"), expected);
    }

    @Test
    void listByRankOutOfRangeSignedLiteral() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.listBin1.[#-2147483649] == 100")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void listIndexHexMaxInt() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT,
                        Exp.val(Integer.MAX_VALUE), Exp.listBin("listBin1")),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin1.[0x7FFFFFFF] == 100"), expected);
    }

    @Test
    void listIndexHexIntOverflow() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.listBin1.[0x80000000] == 100")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void listRankHexIntOverflow() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.listBin1.[#0x80000000] == 100")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void listBinElementEquals_Nested() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(0),
                        CTX.listIndex(0)
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].[0].[0] == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].[0].[0].get(type: INT) == 100"),
                expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].[0].[0].get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void listSize() {
        Exp expected = Exp.eq(
                ListExp.size(Exp.listBin("listBin1")),
                Exp.val(1));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[].count() == 1"), expected);

        // the default behaviour for count() without List '[]' or Map '{}' designators is List
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.count() == 1"), expected);
    }

    @Test
    void nestedListSize() {
        // Without Context
        Exp expected = Exp.eq(
                ListExp.size(
                        ListExp.getByIndex(ListReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val(1),
                                Exp.listBin("listBin1"))
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[1].[].count() == 100"), expected);

        // the default behaviour for count() without List '[]' or Map '{}' designators is List
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[1].count() == 100"), expected);

    }

    @Test
    void nestedListSizeWithContext() {
        // With Context
        Exp expected = Exp.eq(
                ListExp.size(
                        ListExp.getByIndex(ListReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val(2),
                                Exp.listBin("listBin1"),
                                CTX.listIndex(1))
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[1].[2].[].count() == 100"), expected);

        // the default behaviour for count() without List '[]' or Map '{}' designators is List
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[1].[2].count() == 100"), expected);
    }

    @Test
    void nestedLists() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(1),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5)
                ),
                Exp.val("stringVal"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[5].[1].get(type: STRING) == \"stringVal\""),
                expected);
    }

    @Test
    void nestedListsWithDifferentContextTypes() {
        // Nested List Rank
        Exp expected = Exp.eq(
                ListExp.getByRank(
                        ListReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(-1),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5)
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[5].[#-1] == \"stringVal\""), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[5].[#-1].get(type: STRING) == \"stringVal\""),
                expected);

        // Nested List Rank Value
        expected = Exp.eq(
                ListExp.getByValue(
                        ListReturnType.VALUE,
                        Exp.val(100),
                        Exp.listBin("listBin1"),
                        CTX.listIndex(5),
                        CTX.listRank(-1)
                ),
                Exp.val(200));
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[5].[#-1].[=100] == 200"), expected);
    }

    @Test
    void listBinElementCount() {
        Exp expected = Exp.eq(
                ListExp.size(
                        ListExp.getByIndex(ListReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val(0),
                                Exp.listBin("listBin1"))
                ),
                Exp.val(100)
        );
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].count() == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].[].count() == 100"), expected);
    }

    @Test
    void negativeSyntaxList() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.listBin1.[stringValue] == 100")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 12");
    }

    //@Test
    void negativeTypeComparisonList() {
        // TODO: should fail? Exp is successfully created but comparing int to a string value (validations on List)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.listBin1.[#-1].get(type: INT) == \"stringValue\"")))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Cannot invoke");
    }

    @Test
    void listIndexRange() {
        Exp expected = ListExp.getByIndexRange(
                ListReturnType.VALUE,
                Exp.val(1),
                Exp.val(2),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[1:3]"), expected);

        // Negative
        expected = ListExp.getByIndexRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.val(4),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[-3:1]"), expected);

        // Inverted
        expected = ListExp.getByIndexRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(2),
                Exp.val(2),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[!2:4]"), expected);

        // From start till the end
        expected = ListExp.getByIndexRange(
                ListReturnType.VALUE,
                Exp.val(1),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[1:]"), expected);
    }

    @Test
    void listValueList() {
        Exp expected = ListExp.getByValueList(
                ListReturnType.VALUE,
                Exp.val(List.of("a", "b", "c")),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=a,b,c]"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=\"a\",\"b\",\"c\"]"), expected);

        // Integer
        expected = ListExp.getByValueList(
                ListReturnType.VALUE,
                Exp.val(List.of(1, 2, 3)),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=1,2,3]"), expected);

        // Inverted
        expected = ListExp.getByValueList(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(List.of("a", "b", "c")),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[!=a,b,c]"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[!=\"a\",\"b\",\"c\"]"), expected);
    }

    @Test
    void listValueRange() {
        // this operation uses valueEnd instead of count
        Exp expected = ListExp.getByValueRange(
                ListReturnType.VALUE,
                Exp.val(111),
                Exp.val(334),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=111:334]"), expected);

        // Inverted
        expected = ListExp.getByValueRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(10),
                Exp.val(20),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[!=10:20]"), expected);

        // From start till the end
        expected = ListExp.getByValueRange(
                ListReturnType.VALUE,
                Exp.val(111),
                null,
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[=111:]"), expected);
    }

    @Test
    void listRankRange() {
        Exp expected = ListExp.getByRankRange(
                ListReturnType.VALUE,
                Exp.val(0),
                Exp.val(3),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#0:3]"), expected);

        // Inverted
        expected = ListExp.getByRankRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(0),
                Exp.val(3),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[!#0:3]"), expected);

        // From start till the end
        expected = ListExp.getByRankRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-3:]"), expected);

        // From start till the end with context
        expected = ListExp.getByRankRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.listBin("listBin1"),
                CTX.listIndex(5));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[5].[#-3:]"), expected);
    }

    @Test
    void listRankRangeRelative() {
        Exp expected = ListExp.getByValueRelativeRankRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.val("b"),
                Exp.val(2),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-3:-1~b]"), expected);

        // Inverted
        expected = ListExp.getByValueRelativeRankRange(
                ListReturnType.VALUE | ListReturnType.INVERTED,
                Exp.val(-3),
                Exp.val("b"),
                Exp.val(2),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[!#-3:-1~b]"), expected);

        // From start till the end
        expected = ListExp.getByValueRelativeRankRange(
                ListReturnType.VALUE,
                Exp.val(-3),
                Exp.val("b"),
                Exp.listBin("listBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[#-3:~b]"), expected);
    }

    @Test
    void listReturnTypes() {
        Exp expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.COUNT,
                        Exp.Type.LIST,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(5));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(return: COUNT) == 5"), expected);

        expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.EXISTS,
                        Exp.Type.BOOL,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(true));
        // Implicit detect as BOOL
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(return: EXISTS) == true"), expected);

        expected = Exp.eq(
                ListExp.getByIndex(
                        ListReturnType.INDEX,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.listBin("listBin1")
                ),
                Exp.val(1));
        // Implicit detect as INT
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.listBin1.[0].get(return: INDEX) == 1"), expected);
    }

    // ---- List value type contrast ----

    @Test
    void listValueQuotedIntIsString() {
        Exp expectedDQ = Exp.eq(
                ListExp.getByValue(ListReturnType.VALUE,
                        Exp.val("1"), Exp.listBin("listBin")),
                Exp.val("x"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin.[=\"1\"].get(type: STRING) == \"x\""), expectedDQ);
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin.[='1'].get(type: STRING) == 'x'"), expectedDQ);
    }

    @Test
    void listValueUnquotedIntIsInt() {
        Exp expected = Exp.eq(
                ListExp.getByValue(ListReturnType.VALUE,
                        Exp.val(1), Exp.listBin("listBin")),
                Exp.val(10));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin.[=1] == 10"), expected);
    }
}
