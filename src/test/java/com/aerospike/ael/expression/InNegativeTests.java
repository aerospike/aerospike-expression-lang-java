package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InNegativeTests {

    @Test
    void negativeRightOperandString() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in \"Bob\"")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandInt() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in 42")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandFloat() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in 1.5")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandBool() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in true")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandMap() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in {\"a\": 1}")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negativeRightOperandMetadata() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.name in $.ttl()")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negMixedIntAndStringInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [1, \"hello\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedBoolAndIntInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [true, 42]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedFloatAndStringInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [1.5, \"hello\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedIntAndFloatInList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.bin in [1, 1.5]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negPlaceholderResolvesToStr() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of("Bob"))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToInt() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(42))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToFloat() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(1.5))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToBool() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0", PlaceholderValues.of(true))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPlaceholderResolvesToMap() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0",
                        PlaceholderValues.of(Map.of("a", 1)))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negLiteralInMixedTypeList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("\"x\" in [1, \"y\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negPlaceholderInMixedTypeList() {
        // Placeholder value is irrelevant: homogeneity validation fires at parse time before resolution
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("?0 in [1, \"y\"]",
                        PlaceholderValues.of(42))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negPathInMixedTypeList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.name in [1, \"y\"]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negMixedTypeListViaPlaceholder() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?0",
                        PlaceholderValues.of(List.of(1, "hello")))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    @Test
    void negVariableInMixedTypeList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = 1) then (${x} in [1, \"y\"])")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }

    // --- Ambiguous left operand ---

    @Test
    void negBinInBinAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.itemType in $.allowedItems")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInPathAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.rooms.room1.name")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInListDesignatorAmb() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in $.binName.[]")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negBinInPlaceholderAmb() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name in ?0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negVarBoundToInt() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = 1) then (\"100\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToFloat() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = 1.5) then (\"100\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToBool() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = true) then (\"100\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToString() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = \"hello\") then (\"100\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToMap() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = {\"a\": 1}) then (\"100\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToMetadata() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = $.ttl()) then (\"100\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    // --- Variable bound to expression (scalar-producing) ---

    @Test
    void negVarBoundToArithmeticExpr() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = 1 + 2) then (\"foo\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToFunctionExpr() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = abs(1)) then (\"foo\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    // --- Variable bound to explicitly typed bin/path (non-LIST) ---

    @Test
    void negVarBoundToExplicitIntBin() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = $.someBin.get(type: INT)) then (\"foo\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToExplicitStrPath() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = $.a.b.get(type: STRING)) then (\"foo\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    // --- Nested LET variable validation ---

    @Test
    void negNestedLetOuterNonListVar() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 1) then (let(y = [1, 2]) then (\"foo\" in ${x}))")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negNestedShadowedVarLetScalar() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = [1]) then (let(x = 1) then (\"foo\" in ${x}))")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negNotWrappingInLetScalarVar() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 1) then (not(\"foo\" in ${x}))")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarBoundToCountPath() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = $.a.[].count()) then (\"foo\" in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negVarDefLetInScalarVar() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x})) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("variable 'x'");
    }

    @Test
    void negBinInVariableAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = [\"a\"]) then ($.name in ${x})")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPathInBinAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.name in $.list")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPathInPathAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.a in $.rooms.room2.b")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negPathInPlaceholderAmbiguous() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.rooms.room1.name in ?0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negExplicitBinInNotList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?0",
                        PlaceholderValues.of("Bob"))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("placeholder ?0");
    }

    @Test
    void negPlaceholderInNotList() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("?0 in ?1",
                        PlaceholderValues.of("gold", "notAList"))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand")
                .hasMessageContaining("placeholder ?1");
    }

    // --- Missing placeholder values ---

    @Test
    void negPlaceholderMissingValue() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?0", PlaceholderValues.of())))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Missing value for placeholder ?0");
    }

    @Test
    void negPlaceholderIndexOutOfBounds() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("$.name.get(type: STRING) in ?1", PlaceholderValues.of(42))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Missing value for placeholder ?1");
    }

    // --- IN inside WHEN (regression) ---

    @Test
    void negAmbiguousLeftInWhenCond() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.name in $.allowedNames => \"ok\"," +
                        " default => \"no\")")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("cannot infer the type of the left operand for IN operation");
    }

    @Test
    void negNonListRightInWhenCond() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.name.get(type: STRING) in \"Bob\" => \"ok\"," +
                        " default => \"no\")")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN operation requires a List as the right operand");
    }

    @Test
    void negMixedTypeListInWhenCond() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.name in [1, \"hello\"] => \"ok\"," +
                        " default => \"no\")")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("IN list elements must all be of the same type");
    }
}
