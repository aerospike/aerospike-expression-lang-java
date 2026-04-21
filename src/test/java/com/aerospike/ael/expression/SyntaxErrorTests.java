package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SyntaxErrorTests {

    // --- General syntax errors ---

    @Test
    void negNonsensePathFunction() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("1.0 == $.f1.nonsense()")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] extraneous input '()'")
                .hasMessageContaining("at character 20");
    }

    @Test
    void negNonsensePathFunctionOnLeft() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.f1.nonsense() == 1.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] mismatched input '()'")
                .hasMessageContaining("at character 13");
    }

    @Test
    void negTrailingGarbageTokens() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.intBin1 > 100 garbage")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] extraneous input 'garbage'")
                .hasMessageContaining("at character 16");
    }

    // --- Positive syntax boundary ---

    @Test
    void letWithAlphanumericVarName() {
        Exp expected = Exp.let(
                Exp.def("var_1", Exp.val(5)),
                Exp.add(Exp.var("var_1"), Exp.val(1))
        );
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(var_1 = 5) then (${var_1} + 1)"), expected);
    }

    // --- Malformed variable references ---

    @Test
    void negVarBareNameInThenBody() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x})) then (y == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 55");
    }

    @Test
    void negVarDollarNoInThenBody() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x})) then ($y == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Lexer] token recognition error at: '$y'")
                .hasMessageContaining("at character 53")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 56");
    }

    @Test
    void negVarMissingCloseBrace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x)) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Lexer] token recognition error at: '${x)'")
                .hasMessageContaining("at character 40")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 44");
    }

    @Test
    void negVarMissingDollarAndOpen() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in x})) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 41");
    }

    @Test
    void negVarBareNameInLetExpr() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in x)) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 41");
    }

    @Test
    void negVarDoubleOpenBrace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${{x})) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Lexer] token recognition error at: '${{'")
                .hasMessageContaining("at character 40")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 44");
    }

    @Test
    void negVarDoubleCloseBrace() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in ${x}})) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 44");
    }

    @Test
    void negVarDoubleDollarSign() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT) in $${x})) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Lexer] token recognition error at: '$$'")
                .hasMessageContaining("at character 40")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 43");
    }

    // --- Mismatched delimiters ---

    @Test
    void negOrphanedParentheses() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.intBin1 > ()")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] mismatched input '()'")
                .hasMessageContaining("at character 12");
    }

    @Test
    void negGetFuncMissingCloseParen() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of(
                        "let(x = 5, y = ($.bin.get(type: INT in ${x})) then (${y} == true)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 36");
    }

    // --- Malformed let structure ---

    @Test
    void negLetMissingThen() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x = 5) (${x} + 1)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 11");
    }

    @Test
    void negLetEmptyDefs() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let() then (1 + 2)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 3");
    }

    @Test
    void negLetMissingEquals() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("let(x 5) then (${x} + 1)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 6");
    }

    // --- Malformed when structure ---

    @Test
    void negWhenMissingDefault() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.x == 1 => \"a\")")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 20");
    }

    @Test
    void negWhenMissingArrow() {
        assertThatThrownBy(() -> parseFilterExp(
                ExpressionContext.of("when($.x == 1 \"a\", default => \"b\")")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 14");
    }
}
