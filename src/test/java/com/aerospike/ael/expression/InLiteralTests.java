package com.aerospike.ael.expression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.Value;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TreeMap;

import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;

class InLiteralTests {

    @Test
    void stringLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.val(List.of("gold", "silver")));
        parseFilterExpressionAndCompare(ExpressionContext.of("\"gold\" in [\"gold\", \"silver\"]"), expected);
    }

    @Test
    void intLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(100), Exp.val(List.of(100, 200, 300)));
        parseFilterExpressionAndCompare(ExpressionContext.of("100 in [100, 200, 300]"), expected);
    }

    @Test
    void floatLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(1.5), Exp.val(List.of(1.0, 2.0, 3.0)));
        parseFilterExpressionAndCompare(ExpressionContext.of("1.5 in [1.0, 2.0, 3.0]"), expected);
    }

    @Test
    void boolLiteralInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(true), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(ExpressionContext.of("true in [true, false]"), expected);
    }

    @Test
    void listLiteralInListOfLists() {
        List<List<Integer>> outerList = List.of(
                List.of(2, 3, 4), List.of(3, 4, 5), List.of(1, 2, 3), List.of(1, 2));
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(List.of(1, 2, 3)), Exp.val(outerList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("[1,2,3] in [[2,3,4], [3,4,5], [1,2,3], [1,2]]"), expected);
    }

    @Test
    void listBinInListOfLists() {
        List<List<Integer>> outerList = List.of(
                List.of(2, 3, 4), List.of(3, 4, 5), List.of(1, 2, 3), List.of(1, 2));
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.listBin("listBin"), Exp.val(outerList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin in [[2,3,4], [3,4,5], [1,2,3], [1,2]]"), expected);
    }

    @Test
    void mapLiteralInListOfMaps() {
        TreeMap<Integer, String> map = new TreeMap<>();
        map.put(1, "a");
        TreeMap<Integer, String> map2 = new TreeMap<>();
        map2.put(2, "b");
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(map), Exp.val(List.of(map, map2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("{1: \"a\"} in [{1: \"a\"}, {2: \"b\"}]"), expected);
    }

    @Test
    void mapBinInListOfMaps() {
        TreeMap<Integer, String> map1 = new TreeMap<>();
        map1.put(1, "a");
        TreeMap<Integer, String> map2 = new TreeMap<>();
        map2.put(2, "b");
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.mapBin("mapBin"), Exp.val(List.of(map1, map2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin in [{1: \"a\"}, {2: \"b\"}]"), expected);
    }

    @Test
    void binInStringListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name in [\"Bob\", \"Mary\"]"), expected);
    }

    @Test
    void binInIntListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("age"), Exp.val(List.of(18, 21, 65)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.age in [18, 21, 65]"), expected);
    }

    @Test
    void binInFloatListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("score"), Exp.val(List.of(1.0, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.score in [1.0, 2.5]"), expected);
    }

    @Test
    void binInBoolListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("isActive"), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.isActive in [true, false]"), expected);
    }

    @Test
    void nestedPathInListLiteral() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                MapExp.getByKey(
                        MapReturnType.VALUE,
                        Exp.Type.STRING,
                        Exp.val("rateType"),
                        Exp.mapBin("rooms"),
                        CTX.mapKey(Value.get("room1")),
                        CTX.mapKey(Value.get("rates"))),
                Exp.val(List.of("RACK_RATE", "DISCOUNT")));
        parseFilterExpressionAndCompare(ExpressionContext.of(
                "$.rooms.room1.rates.rateType in [\"RACK_RATE\", \"DISCOUNT\"]"), expected);
    }

    @Test
    void explicitIntBinInListDesignator() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("name"), Exp.listBin("binName"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: INT) in $.binName.[]"), expected);
    }

    @Test
    void inWithSingleElementList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.name in [\"Bob\"]"), expected);
    }

    @Test
    void inWithNegativeInts() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.val(List.of(-1, 0, 1)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.val in [-1, 0, 1]"), expected);
    }

    @Test
    void inWithNegativeFloats() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.val(List.of(-1.5, 0.0, 1.5)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.val in [-1.5, 0.0, 1.5]"), expected);
    }

    @Test
    void inWithHexBinaryLiterals() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.val(List.of(255, 5, 42)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.val in [0xFF, 0b101, 42]"), expected);
    }
}
