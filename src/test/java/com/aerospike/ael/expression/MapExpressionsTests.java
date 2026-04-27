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

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MapExpressionsTests {

    @Test
    void mapOneLevelExpressions() {
        // Int
        Exp expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(200));
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a == 200"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(type: INT) == 200"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(type: INT, return: VALUE) == 200"),
                expected);

        // String
        expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a == \"stringVal\""), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(type: STRING) == \"stringVal\""),
                expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(type: STRING, return: VALUE) ==" +
                " \"stringVal\""), expected);
    }

    @Test
    void mapNestedLevelExpressions() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc > 200"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc.get(type: INT) > 200"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc.get(type: INT, return: VALUE) > 200"),
                expected);

        // String
        expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc == \"stringVal\""), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc.get(type: STRING) == \"stringVal\""),
                expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc.get(type: STRING, return: VALUE)" +
                " == \"stringVal\""), expected);
    }

    @Test
    void quotedStringInExpressionPath() {
        Exp expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a")), CTX.mapKey(Value.get("bb"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.bb.bcc.get(type: INT) > 200"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.\"bb\".bcc.get(type: INT) > 200"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.'bb'.bcc.get(type: INT) > 200"), expected);

        expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("127.0.0.1"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.\"127.0.0.1\".bcc.get(type: INT) > 200"),
                expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.'127.0.0.1'.bcc.get(type: INT) > 200"),
                expected);


        expected = Exp.gt(
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val("bcc"),
                        Exp.mapBin("mapBin1"),
                        // Map key with spaces in it
                        CTX.mapKey(Value.get("127 0 0 1"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.\"127 0 0 1\".bcc.get(type: INT) > 200"),
                expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.'127 0 0 1'.bcc.get(type: INT) > 200"),
                expected);
    }

    @Test
    void mapSize() {
        Exp expected = Exp.gt(
                MapExp.size(
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{}.count() > 200"), expected);

        // the default behaviour for count() without List '[]' or Map '{}' designators is List
        Exp expected2 = Exp.gt(
                ListExp.size(
                        Exp.listBin("mapBin1")
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.count() > 200"), expected2);
    }

    @Test
    void nestedMapSize() {
        // Without Context
        Exp expected = Exp.eq(
                MapExp.size(
                        MapExp.getByKey(ListReturnType.VALUE,
                                Exp.Type.MAP,
                                Exp.val("a"),
                                Exp.mapBin("mapBin1"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.{}.count() == 200"), expected);

        // the default behaviour for count() without Map '{}' or List '[]' designators is List
        Exp expected2 = Exp.eq(
                ListExp.size(
                        MapExp.getByKey(MapReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val("a"),
                                Exp.mapBin("mapBin1"))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.count() == 200"), expected2);
    }

    @Test
    void nestedMapSizeWithContext() {
        // With Context
        Exp expected = Exp.eq(
                MapExp.size(
                        MapExp.getByKey(ListReturnType.VALUE,
                                Exp.Type.MAP,
                                Exp.val("b"),
                                Exp.mapBin("mapBin1"),
                                CTX.mapKey(Value.get("a")))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.b.{}.count() == 200"), expected);

        // the default behaviour for count() without Map '{}' or List '[]' designators is List
        Exp expected2 = Exp.eq(
                ListExp.size(
                        MapExp.getByKey(MapReturnType.VALUE,
                                Exp.Type.LIST,
                                Exp.val("b"),
                                Exp.mapBin("mapBin1"),
                                CTX.mapKey(Value.get("a")))
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.b.count() == 200"), expected2);
    }

    @Test
    void mapByIndex() {
        Exp expected = Exp.eq(
                MapExp.getByIndex(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(0),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0} == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0}.get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0}.get(type: INT, return: VALUE) == 100"),
                expected);

        Exp expected2 = Exp.eq(
                MapExp.getByIndex(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(0),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val("value"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0} == 'value'"), expected2);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0}.get(type: STRING) == 'value'"), expected2);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0}.get(type: STRING, return: VALUE) == 'value'"),
                expected2);
    }

    @Test
    void mapByValue() {
        Exp expected = Exp.eq(
                MapExp.getByValue(
                        MapReturnType.VALUE,
                        Exp.val(100),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=100} == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=100}.get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=100}.get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void mapByValueHexAndBinarySelectors() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=0xff} == 100"),
                Exp.eq(MapExp.getByValue(MapReturnType.VALUE, Exp.val(255), Exp.mapBin("mapBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=0b10} == 100"),
                Exp.eq(MapExp.getByValue(MapReturnType.VALUE, Exp.val(2), Exp.mapBin("mapBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=-0xff} == 100"),
                Exp.eq(MapExp.getByValue(MapReturnType.VALUE, Exp.val(-255), Exp.mapBin("mapBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=-0b10} == 100"),
                Exp.eq(MapExp.getByValue(MapReturnType.VALUE, Exp.val(-2), Exp.mapBin("mapBin1")), Exp.val(100)));
    }

    @Test
    void mapByValueIntegerMinDecimal() {
        Exp expected = Exp.eq(
                MapExp.getByValue(
                        MapReturnType.VALUE,
                        Exp.val(Integer.MIN_VALUE),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=-2147483648} == 100"), expected);
    }

    @Test
    void mapByValueCount() {
        Exp expected = Exp.gt(
                MapExp.getByValue(MapReturnType.COUNT,
                        Exp.val(100),
                        Exp.mapBin("mapBin1")),
                Exp.val(0)
        );
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=100}.count() > 0"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=100}.{}.count() > 0"), expected);
    }

    @Test
    void mapByRank() {
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-1} == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-1}.get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-1}.get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void mapByRankHexAndBinarySelectors() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#0xff} == 100"),
                Exp.eq(MapExp.getByRank(MapReturnType.VALUE, Exp.Type.INT, Exp.val(255), Exp.mapBin("mapBin1")),
                        Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#0b10} == 100"),
                Exp.eq(MapExp.getByRank(MapReturnType.VALUE, Exp.Type.INT, Exp.val(2), Exp.mapBin("mapBin1")),
                        Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-0xff} == 100"),
                Exp.eq(MapExp.getByRank(MapReturnType.VALUE, Exp.Type.INT, Exp.val(-255), Exp.mapBin("mapBin1")),
                        Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-0b10} == 100"),
                Exp.eq(MapExp.getByRank(MapReturnType.VALUE, Exp.Type.INT, Exp.val(-2), Exp.mapBin("mapBin1")),
                        Exp.val(100)));
    }

    @Test
    void mapIndexHexAndBinary() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0xff} == 100"),
                Exp.eq(MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.INT, Exp.val(255),
                        Exp.mapBin("mapBin1")), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{-0b10} == 100"),
                Exp.eq(MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.INT, Exp.val(-2),
                        Exp.mapBin("mapBin1")), Exp.val(100)));
    }

    @Test
    void mapIndexRangeHex() {
        Exp expected = MapExp.getByIndexRange(
                MapReturnType.VALUE, Exp.val(1), Exp.val(2), Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin1.{0x01:0x03}"), expected);
    }

    @Test
    void mapRankRangeHex() {
        Exp expected = MapExp.getByRankRange(
                MapReturnType.VALUE, Exp.val(1), Exp.val(2), Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin1.{#0x01:0x03}"), expected);
    }

    @Test
    void mapValueRangeHex() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE, Exp.val(255), Exp.val(512), Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin1.{=0xff:0x200}"), expected);
    }

    @Test
    void mapByRankIntegerMinDecimal() {
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(Integer.MIN_VALUE),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-2147483648} == 100"), expected);
    }

    @Test
    void mapIndexHexMaxInt() {
        Exp expected = Exp.eq(
                MapExp.getByIndex(MapReturnType.VALUE, Exp.Type.INT,
                        Exp.val(Integer.MAX_VALUE), Exp.mapBin("mapBin1")),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin1.{0x7FFFFFFF} == 100"), expected);
    }

    @Test
    void mapIndexHexIntOverflow() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.mapBin1.{0x80000000} == 100")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void mapRankHexIntOverflow() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.mapBin1.{#0x80000000} == 100")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void mapByRankWithNesting() {
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.INT,
                        Exp.val(-1),
                        Exp.mapBin("mapBin1"),
                        CTX.mapKey(Value.get("a"))
                ),
                Exp.val(100));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.{#-1} == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.{#-1}.get(type: INT) == 100"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.{#-1}.get(type: INT, return: VALUE) == 100"),
                expected);
    }

    @Test
    void nestedListsWithDifferentContextTypes() {
        // Nested List Rank
        Exp expected = Exp.eq(
                MapExp.getByRank(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val(-1),
                        Exp.mapBin("mapBin1"),
                        CTX.mapIndex(5)
                ),
                Exp.val("stringVal"));
        // Implicit detect as String
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{5}.{#-1} == \"stringVal\""), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{5}.{#-1}.get(type: STRING) == \"stringVal\""),
                expected);

        // Nested List Rank Value
        expected = Exp.eq(
                MapExp.getByValue(
                        MapReturnType.VALUE,
                        Exp.val(100),
                        Exp.mapBin("mapBin1"),
                        CTX.mapIndex(5),
                        CTX.mapRank(-1)
                ),
                Exp.val(200));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{5}.{#-1}.{=100} == 200"), expected);
    }

    @Test
    void mapKeyRange() {
        Exp expected = MapExp.getByKeyRange(
                MapReturnType.VALUE,
                Exp.val("a"),
                Exp.val("c"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{a-c}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{\"a\"-\"c\"}"), expected);

        // Inverted
        expected = MapExp.getByKeyRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("a"),
                Exp.val("c"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!a-c}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!\"a\"-\"c\"}"), expected);

        // From start till the end
        expected = MapExp.getByKeyRange(
                MapReturnType.VALUE,
                Exp.val("a"),
                null,
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{a-}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{\"a\"-}"), expected);
    }

    @Test
    void mapKeyList() {
        Exp expected = MapExp.getByKeyList(
                MapReturnType.VALUE,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{a,b,c}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{\"a\",\"b\",\"c\"}"), expected);

        // Inverted
        expected = MapExp.getByKeyList(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!a,b,c}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!\"a\",\"b\",\"c\"}"), expected);
    }

    @Test
    void mapIndexRange() {
        Exp expected = MapExp.getByIndexRange(
                MapReturnType.VALUE,
                Exp.val(1),
                Exp.val(2),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{1:3}"), expected);

        // Negative
        expected = MapExp.getByIndexRange(
                MapReturnType.VALUE,
                Exp.val(-3),
                Exp.val(4),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{-3:1}"), expected);

        // Inverted
        expected = MapExp.getByIndexRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(2),
                Exp.val(2),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!2:4}"), expected);

        // From start till the end
        expected = MapExp.getByIndexRange(
                MapReturnType.VALUE,
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{1:}"), expected);
    }

    @Test
    void mapValueList() {
        Exp expected = MapExp.getByValueList(
                MapReturnType.VALUE,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=a,b,c}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=\"a\",\"b\",\"c\"}"), expected);

        // Integer
        expected = MapExp.getByValueList(
                MapReturnType.VALUE,
                Exp.val(List.of(1, 2, 3)),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=1,2,3}"), expected);

        // Inverted
        expected = MapExp.getByValueList(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(List.of("a", "b", "c")),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!=a,b,c}"), expected);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!=\"a\",\"b\",\"c\"}"), expected);
    }

    @Test
    void mapValueRange() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val(111),
                Exp.val(334),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=111:334}"), expected);

        // Inverted
        expected = MapExp.getByValueRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(10),
                Exp.val(20),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!=10:20}"), expected);

        // From start till the end
        expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val(111),
                null,
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=111:}"), expected);
    }

    @Test
    void mapValueRangeStrUnquoted() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val("aaa"),
                Exp.val("zzz"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=aaa:zzz}"), expected);
    }

    @Test
    void mapValueRangeStrSingleQuoted() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val("hello"),
                Exp.val("world"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{='hello':'world'}"), expected);
    }

    @Test
    void mapValueRangeStrDoubleQuoted() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val("hello"),
                Exp.val("world"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=\"hello\":\"world\"}"), expected);
    }

    @Test
    void mapValueRangeStrOpenEnded() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val("hello"),
                null,
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{='hello':}"), expected);
    }

    @Test
    void mapValueRangeStrInverted() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("hello"),
                Exp.val("world"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!='hello':'world'}"), expected);
    }

    @Test
    void mapValueRangeMixedTypes() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val("abc"),
                Exp.val(999),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=abc:999}"), expected);
    }

    @Test
    void mapValueRangeBlob() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val(new byte[]{0x0A}),
                Exp.val(new byte[]{(byte) 0xFF}),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=X'0A':X'FF'}"), expected);
    }

    @Test
    void mapValueRangeBlobB64() {
        byte[] start = java.util.Base64.getDecoder().decode("AAAA");
        byte[] end = java.util.Base64.getDecoder().decode("////");
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val(start),
                Exp.val(end),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=b64'AAAA':b64'////'}"), expected);
    }

    @Test
    void mapValueRangeInKeyword() {
        Exp expected = MapExp.getByValueRange(
                MapReturnType.VALUE,
                Exp.val("in"),
                Exp.val("out"),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{=in:out}"), expected);
    }

    @Test
    void mapRankRange() {
        Exp expected = MapExp.getByRankRange(
                MapReturnType.VALUE,
                Exp.val(0),
                Exp.val(3),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#0:3}"), expected);

        // Inverted
        expected = MapExp.getByRankRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(0),
                Exp.val(3),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!#0:3}"), expected);

        // From start till the end
        expected = MapExp.getByRankRange(
                MapReturnType.VALUE,
                Exp.val(-3),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-3:}"), expected);

        // From start till the end with context
        expected = MapExp.getByRankRange(
                MapReturnType.VALUE,
                Exp.val(-3),
                Exp.mapBin("mapBin1"),
                CTX.mapIndex(5));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{5}.{#-3:}"), expected);
    }

    @Test
    void mapRankRangeRelative() {
        Exp expected = MapExp.getByValueRelativeRankRange(
                MapReturnType.VALUE,
                Exp.val(-1),
                Exp.val(10),
                Exp.val(2),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-1:1~10}"), expected);

        // Inverted
        expected = MapExp.getByValueRelativeRankRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val(-1),
                Exp.val(10),
                Exp.val(2),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!#-1:1~10}"), expected);

        // From start till the end
        expected = MapExp.getByValueRelativeRankRange(
                MapReturnType.VALUE,
                Exp.val(-2),
                Exp.val(10),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{#-2:~10}"), expected);
    }

    @Test
    void mapIndexRangeRelative() {
        Exp expected = MapExp.getByKeyRelativeIndexRange(
                MapReturnType.VALUE,
                Exp.val("a"),
                Exp.val(0),
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0:1~a}"), expected);

        // Inverted
        expected = MapExp.getByKeyRelativeIndexRange(
                MapReturnType.VALUE | MapReturnType.INVERTED,
                Exp.val("a"),
                Exp.val(0),
                Exp.val(1),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{!0:1~a}"), expected);

        // From start till the end
        expected = MapExp.getByKeyRelativeIndexRange(
                MapReturnType.VALUE,
                Exp.val("a"),
                Exp.val(0),
                Exp.mapBin("mapBin1"));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.{0:~a}"), expected);
    }

    @Test
    void mapReturnTypes() {
        Exp expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.COUNT,
                        Exp.Type.INT,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(5));
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(type: INT, return: COUNT) == 5"),
                expected);

        expected = MapExp.getByKey(
                MapReturnType.ORDERED_MAP,
                Exp.Type.STRING,
                Exp.val("a"),
                Exp.mapBin("mapBin1")
        );
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(return: ORDERED_MAP)"), expected);

        expected = Exp.eq(
                MapExp.getByKey(
                        MapReturnType.RANK,
                        Exp.Type.INT,
                        Exp.val("a"),
                        Exp.mapBin("mapBin1")
                ),
                Exp.val(5));
        // Implicit detect as Int
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.mapBin1.a.get(type: INT, return: RANK) == 5"),
                expected);
    }

    // ---- Map value type contrast ----

    @Test
    void mapValueQuotedIntIsString() {
        Exp expectedDQ = Exp.eq(
                MapExp.getByValue(MapReturnType.VALUE,
                        Exp.val("1"), Exp.mapBin("mapBin")),
                Exp.val("x"));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin.{=\"1\"}.get(type: STRING) == \"x\""), expectedDQ);
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin.{='1'}.get(type: STRING) == 'x'"), expectedDQ);
    }

    @Test
    void mapValueUnquotedIntIsInt() {
        Exp expected = Exp.eq(
                MapExp.getByValue(MapReturnType.VALUE,
                        Exp.val(1), Exp.mapBin("mapBin")),
                Exp.val(10));
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin.{=1} == 10"), expected);
    }
}
