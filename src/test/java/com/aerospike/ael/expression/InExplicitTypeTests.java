package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InExplicitTypeTests {

    @Test
    void explicitListTypeOnRightBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.listBin("tags"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.intBin1.get(type: INT) in $.tags.get(type: LIST)"), expected);
    }

    @Test
    void explicitIntInIntList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("age"), Exp.val(List.of(1, 2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.age.get(type: INT) in [1, 2]"), expected);
    }

    @Test
    void explicitStringInStringList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("a")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: STRING) in [\"a\"]"), expected);
    }

    @Test
    void explicitIntCompatibleWithFloatList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.val(List.of(1.5, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: INT) in [1.5, 2.5]"), expected);
    }

    @Test
    void explicitFloatCompatibleWithIntList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.val(List.of(1, 2)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: FLOAT) in [1, 2]"), expected);
    }

    @Test
    void explicitFloatInFloatList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.val(List.of(1.5, 2.5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: FLOAT) in [1.5, 2.5]"), expected);
    }

    @Test
    void explicitBoolInBoolList() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("flag"), Exp.val(List.of(true, false)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.flag.get(type: BOOL) in [true, false]"), expected);
    }

    // --- Explicit type on left BIN_PART, right is a bin ---

    @Test
    void explicitStringBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: STRING) in $.list"), expected);
    }

    @Test
    void explicitIntBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: INT) in $.list"), expected);
    }

    @Test
    void explicitFloatBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: FLOAT) in $.list"), expected);
    }

    @Test
    void explicitBoolBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("flag"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.flag.get(type: BOOL) in $.list"), expected);
    }

    @Test
    void explicitListBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.listBin("items"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.items.get(type: LIST) in $.list"), expected);
    }

    @Test
    void explicitMapBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.mapBin("item"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.item.get(type: MAP) in $.list"), expected);
    }

    // --- Explicit type on left BIN_PART, right is a path operand ---

    @Test
    void explicitStringBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: STRING) in $.items.tags"), expected);
    }

    @Test
    void explicitIntBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: INT) in $.items.tags"), expected);
    }

    @Test
    void explicitFloatBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.floatBin("val"),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: FLOAT) in $.items.tags"), expected);
    }

    @Test
    void explicitBoolBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.boolBin("flag"),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.flag.get(type: BOOL) in $.items.tags"), expected);
    }

    @Test
    void explicitListBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.listBin("items"),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.items.get(type: LIST) in $.items.tags"), expected);
    }

    @Test
    void explicitMapBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.mapBin("item"),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.item.get(type: MAP) in $.items.tags"), expected);
    }

    // --- Cast on left BIN_PART ---

    @Test
    void castIntBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.toInt(Exp.floatBin("val")), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.asInt() in $.list"), expected);
    }

    @Test
    void castFloatBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.toFloat(Exp.intBin("val")), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.asFloat() in $.list"), expected);
    }

    @Test
    void castIntBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.toInt(Exp.floatBin("val")),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.asInt() in $.items.tags"), expected);
    }

    @Test
    void castFloatBinInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.toFloat(Exp.intBin("val")),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("tags"), Exp.mapBin("items")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.asFloat() in $.items.tags"), expected);
    }

    // --- Explicit type on left PATH_OPERAND ---

    @Test
    void explicitPathInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("name"), Exp.mapBin("rooms")),
                Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.rooms.name.get(type: STRING) in $.list"), expected);
    }

    @Test
    void explicitPathInPath() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("name"), Exp.mapBin("rooms")),
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("list"), Exp.mapBin("rooms2")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.rooms.name.get(type: STRING) in $.rooms2.list"), expected);
    }

    // --- Explicit type on both sides ---

    @Test
    void explicitBinInExplicitBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("val"), Exp.listBin("tags"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.val.get(type: INT) in $.tags.get(type: LIST)"), expected);
    }

    // --- Explicit type with placeholder right ---

    @Test
    void explicitBinInPlaceholder() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: STRING) in ?0",
                        PlaceholderValues.of(List.of("Bob"))), expected);
    }

    @Test
    void explicitPathInPlaceholder() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING,
                        Exp.val("name"), Exp.mapBin("rooms")),
                Exp.val(List.of("Bob")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.rooms.name.get(type: STRING) in ?0",
                        PlaceholderValues.of(List.of("Bob"))), expected);
    }

    // --- Explicit type with list-designator right ---

    @Test
    void explicitBinInListDesignator() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("name"), Exp.listBin("binName"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.name.get(type: INT) in $.binName.[]"), expected);
    }

    // --- Placeholder left (concrete value, not ambiguous) ---

    @Test
    void posBothPlaceholders() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val("gold"), Exp.val(List.of("gold", "silver")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in ?1",
                        PlaceholderValues.of("gold", List.of("gold", "silver"))), expected);
    }

    @Test
    void posPlaceholderInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.val(42), Exp.listBin("bin"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("?0 in $.bin",
                        PlaceholderValues.of(42)), expected);
    }

    // --- Variable left (concrete value, not ambiguous) ---

    @Test
    void posVariableInBin() {
        Exp expected = Exp.let(
                Exp.def("x", Exp.val(1)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.var("x"), Exp.listBin("list")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(x = 1) then (${x} in $.list)"), expected);
    }

    // --- PATH_OPERAND with type designator (not ambiguous) ---

    @Test
    void listDesignatorBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.listBin("items"), Exp.listBin("list"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.items.[] in $.list"), expected);
    }

    @Test
    void mapDesignatorBinInBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.mapBin("item"), Exp.listBin("list")
        );
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.item.{} in $.list"), expected);
    }

    // --- PATH_OPERAND with COUNT/SIZE function (known INT return) ---

    @Test
    void countPathInListBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                ListExp.size(
                        ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.LIST,
                                Exp.val(0), Exp.listBin("items"))
                ),
                Exp.listBin("list")
        );
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.items.[0].count() in $.list"), expected);
    }

    @Test
    void countListBin() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                ListExp.size(Exp.listBin("items")),
                Exp.listBin("list")
        );
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.items.[].count() in $.list"), expected);
    }

    // --- Negative: explicit type on right, non-LIST ---

    @Test
    void negExplicitIntTypeOnRightBin() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.tags.get(type: INT)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negExplicitStringTypeOnRightBin() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.tags.get(type: STRING)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negNestedPathExplicitStringOnRight() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.tags.nested.get(type: STRING)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negExplicitStringInIntList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in [1, 2]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitIntInStringList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.age.get(type: INT) in [\"a\", \"b\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitBoolInIntList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.flag.get(type: BOOL) in [1, 2]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitIntInBoolList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.val.get(type: INT) in [true, false]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitFloatInStringList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.val.get(type: FLOAT) in [\"a\", \"b\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitFloatInBoolList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.val.get(type: FLOAT) in [true, false]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitStringInFloatList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in [1.5, 2.5]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitStringInBoolList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in [true, false]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitBoolInStringList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.flag.get(type: BOOL) in [\"a\", \"b\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }

    @Test
    void negExplicitBoolInFloatList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.flag.get(type: BOOL) in [1.5, 2.5]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Cannot compare");
    }
}
