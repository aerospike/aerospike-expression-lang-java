package com.aerospike.ael.expression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;

class InPlaceholderTests {

    @Test
    void placeholderAsLeftOperand() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.val(List.of("gold", "silver")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [\"gold\", \"silver\"]",
                        PlaceholderValues.of("gold")), expected);
    }

    @Test
    void intPlaceholderAsLeftOperand() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(100), Exp.val(List.of(100, 200, 300)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [100, 200, 300]",
                        PlaceholderValues.of(100)), expected);
    }

    @Test
    void floatPlaceholderAsLeftOperand() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(1.5), Exp.val(List.of(1.0, 2.0, 3.0)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [1.0, 2.0, 3.0]",
                        PlaceholderValues.of(1.5)), expected);
    }

    @Test
    void boolPlaceholderAsLeftOperand() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(true), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [true, false]",
                        PlaceholderValues.of(true)), expected);
    }

    @Test
    void listPlaceholderAsLeftOperand() {
        List<List<Integer>> outerList = List.of(
                List.of(1, 2, 3), List.of(4, 5, 6));
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(List.of(1, 2, 3)), Exp.val(outerList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [[1,2,3], [4,5,6]]",
                        PlaceholderValues.of(List.of(1, 2, 3))), expected);
    }

    @Test
    void mapPlaceholderAsLeftOperand() {
        TreeMap<Integer, String> map = new TreeMap<>();
        map.put(1, "a");
        TreeMap<Integer, String> map2 = new TreeMap<>();
        map2.put(2, "b");
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(map), Exp.val(List.of(map, map2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in [{1: \"a\"}, {2: \"b\"}]",
                        PlaceholderValues.of(map)), expected);
    }

    @Test
    void placeholderAsRightOperand() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: STRING) in ?0",
                        PlaceholderValues.of(List.of("Bob", "Mary"))), expected);
    }

    @Test
    void intListPlaceholderAsRight() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("age"), Exp.val(List.of(1, 2, 3)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.age.get(type: INT) in ?0",
                        PlaceholderValues.of(List.of(1, 2, 3))), expected);
    }

    @Test
    void floatListPlaceholderAsRight() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("score"), Exp.val(List.of(1.5, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.score.get(type: FLOAT) in ?0",
                        PlaceholderValues.of(List.of(1.5, 2.5))), expected);
    }

    @Test
    void boolListPlaceholderAsRight() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("isActive"), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.isActive.get(type: BOOL) in ?0",
                        PlaceholderValues.of(List.of(true, false))), expected);
    }

    @Test
    void listOfListsPlaceholderAsRight() {
        List<List<Integer>> outerList = List.of(
                List.of(1, 2, 3), List.of(4, 5, 6));
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.listBin("listBin"), Exp.val(outerList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.listBin.get(type: LIST) in ?0",
                        PlaceholderValues.of(outerList)), expected);
    }

    @Test
    void mapListPlaceholderAsRight() {
        TreeMap<Integer, String> map1 = new TreeMap<>();
        map1.put(1, "a");
        TreeMap<Integer, String> map2 = new TreeMap<>();
        map2.put(2, "b");
        List<TreeMap<Integer, String>> mapList = List.of(map1, map2);
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.mapBin("mapBin"), Exp.val(mapList));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.mapBin.get(type: MAP) in ?0",
                        PlaceholderValues.of(mapList)), expected);
    }

    @Test
    void emptyListPlaceholderAsRight() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(Collections.emptyList()));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.intBin1.get(type: INT) in ?0",
                        PlaceholderValues.of(Collections.emptyList())), expected);
    }

    @Test
    void bothPlaceholders() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.val(List.of("gold", "silver")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in ?1",
                        PlaceholderValues.of("gold", List.of("gold", "silver"))), expected);
    }
}
