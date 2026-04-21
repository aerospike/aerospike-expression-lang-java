package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for numeric literal parsing: INT (decimal, hex, binary) and FLOAT.
 * <p>
 * INT spec: optional sign (+/-), hex (0x/0X), binary (0b/0B), decimal digits.
 * FLOAT spec: optional sign (+/-), sequence of digits and exactly one decimal separator
 * (not the last char). Leading-dot floats (.37, -.37, +.37) are supported.
 */
public class NumericLiteralsTests {

    // ==================== INT: decimal ====================

    @Test
    void intDecimalPositive() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("5 == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void intDecimalNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-5 == -5"),
                Exp.eq(Exp.val(-5), Exp.val(-5)));
    }

    @Test
    void intDecimalPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+5 == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void intDecimalZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0 == 0"),
                Exp.eq(Exp.val(0), Exp.val(0)));
    }

    @Test
    void intDecimalLongMinViaUnaryMinus() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("-9223372036854775808 == -9223372036854775808"),
                Exp.eq(Exp.val(Long.MIN_VALUE), Exp.val(Long.MIN_VALUE)));
    }

    // ==================== INT: hexadecimal ====================

    @Test
    void intHexLowercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0xff == 255"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexUppercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0XFF == 255"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-0xff == -255"),
                Exp.eq(Exp.val(-255), Exp.val(-255)));
    }

    @Test
    void intHexPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+0xff == 255"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0x0 == 0"),
                Exp.eq(Exp.val(0), Exp.val(0)));
    }

    @Test
    void intHexCaseInsensitive() {
        // Mixed-case hex digits should parse identically
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0xff == 0xFF"),
                Exp.eq(Exp.val(255), Exp.val(255)));
    }

    @Test
    void intHexLongMinViaUnaryMinus() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("-0x8000000000000000 == -9223372036854775808"),
                Exp.eq(Exp.val(Long.MIN_VALUE), Exp.val(Long.MIN_VALUE)));
    }

    // ==================== INT: binary ====================

    @Test
    void intBinaryLowercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0b1010 == 10"),
                Exp.eq(Exp.val(10), Exp.val(10)));
    }

    @Test
    void intBinaryUppercase() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0B1010 == 10"),
                Exp.eq(Exp.val(10), Exp.val(10)));
    }

    @Test
    void intBinaryNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-0b1010 == -10"),
                Exp.eq(Exp.val(-10), Exp.val(-10)));
    }

    @Test
    void intBinaryPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+0b1010 == 10"),
                Exp.eq(Exp.val(10), Exp.val(10)));
    }

    @Test
    void intBinaryZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0b0 == 0"),
                Exp.eq(Exp.val(0), Exp.val(0)));
    }

    @Test
    void intBinaryLongMinViaUnaryMinus() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of(
                        "-0b1000000000000000000000000000000000000000000000000000000000000000 == -9223372036854775808"
                ),
                Exp.eq(Exp.val(Long.MIN_VALUE), Exp.val(Long.MIN_VALUE)));
    }

    // ==================== FLOAT ====================

    @Test
    void floatStandard() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("3.14 == 3.14"),
                Exp.eq(Exp.val(3.14), Exp.val(3.14)));
    }

    @Test
    void floatNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-34.1 == -34.1"),
                Exp.eq(Exp.val(-34.1), Exp.val(-34.1)));
    }

    @Test
    void floatPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+3.14 == 3.14"),
                Exp.eq(Exp.val(3.14), Exp.val(3.14)));
    }

    @Test
    void floatZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("0.0 == 0.0"),
                Exp.eq(Exp.val(0.0), Exp.val(0.0)));
    }

    @Test
    void floatPositiveZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+0.0 == 0.0"),
                Exp.eq(Exp.val(0.0), Exp.val(0.0)));
    }

    // ==================== Sign behavior with operators ====================

    @Test
    void plusAsOperatorWithSpaces() {
        // Verify that '+' with spaces is treated as an addition operator, not a sign
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5 + 3) == 8"),
                Exp.eq(Exp.add(Exp.val(5), Exp.val(3)), Exp.val(8)));
    }

    @Test
    void minusAsOperatorWithSpaces() {
        // Verify that '-' with spaces is treated as a subtraction operator, not a sign
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5 - 3) == 2"),
                Exp.eq(Exp.sub(Exp.val(5), Exp.val(3)), Exp.val(2)));
    }

    @Test
    void plusAsOperatorWithoutSpaces() {
        // '5+3' must tokenize as INT(5) '+' INT(3), not INT(5) INT(+3)
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5+3) == 8"),
                Exp.eq(Exp.add(Exp.val(5), Exp.val(3)), Exp.val(8)));
    }

    @Test
    void minusAsOperatorWithoutSpaces() {
        // '5-3' must tokenize as INT(5) '-' INT(3), not INT(5) INT(-3)
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5-3) == 2"),
                Exp.eq(Exp.sub(Exp.val(5), Exp.val(3)), Exp.val(2)));
    }

    @Test
    void mixedSpacelessOperators() {
        // Multiple spaceless operators in one expression
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(10-3+1) == 8"),
                Exp.eq(Exp.add(Exp.sub(Exp.val(10), Exp.val(3)), Exp.val(1)), Exp.val(8)));
    }

    @Test
    void spacelessSubtractionOfNegative() {
        // '5 - -3' should parse as subtraction of a negated literal
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(5 - -3) == 8"),
                Exp.eq(Exp.sub(Exp.val(5), Exp.val(-3)), Exp.val(8)));
    }

    @Test
    void doubleNegation() {
        // '--5' should parse as two unary minuses, resulting in 5
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("--5 == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void unaryPlusBeforeParenthesized() {
        // Unary '+' before a parenthesized expression is a no-op
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+(5) == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void doubleUnaryPlusInt() {
        // '++5' should parse as two unary plus operators, resulting in 5
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("++5 == 5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    @Test
    void doubleUnaryPlusIntOnRightOperand() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("5 == ++5"),
                Exp.eq(Exp.val(5), Exp.val(5)));
    }

    // ==================== Negative / error tests ====================

    @Test
    void invalidHexDigits() {
        // 0xGG is not valid hex -- lexer produces INT(0) + NAME_IDENTIFIER(xGG)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0xGG == 0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 5");
    }

    @Test
    void invalidBinaryDigits() {
        // 0b2 is not valid binary -- lexer produces INT(0) + NAME_IDENTIFIER(b2)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0b2 == 0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 4");
    }

    @Test
    void trailingDotFloat() {
        // "10." is not valid FLOAT -- requires digits after dot
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("10. == 10.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 4");
    }

    @Test
    void leadingDotHexIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of(".0x10 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == .0x10")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithMinusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("-.0x10 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithMinusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == -.0x10")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithPlusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("+.0x10 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithPlusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == +.0x10")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithDoubleMinusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("--.0x10 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithDoubleMinusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == --.0x10")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithDoublePlusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("++.0x10 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotHexWithDoublePlusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == ++.0x10")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of(".0b11 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == .0b11")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithMinusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("-.0b11 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithMinusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == -.0b11")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithPlusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("+.0b11 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithPlusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == +.0b11")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithDoubleMinusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("--.0b11 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithDoubleMinusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == --.0b11")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithDoublePlusSignIsRejected() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("++.0b11 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void leadingDotBinaryWithDoublePlusSignIsRejectedOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == ++.0b11")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void decimalIntOverflowPositive() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("9223372036854775808 == 0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void decimalIntOverflowNegative() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("-9223372036854775809 == 0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void decimalIntOverflowDoubleUnaryMinusLongMin() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("--9223372036854775808 == 0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void decimalIntOverflowDoubleUnaryMinusLongMinCast() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("--9223372036854775808.asFloat() == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void hexIntOverflowPositive() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0x8000000000000001 == 0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("out of range");
    }

    // ==================== FLOAT: leading-dot ====================

    @Test
    void leadingDotFloat() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of(".37 == 0.37"),
                Exp.eq(Exp.val(0.37), Exp.val(0.37)));
    }

    @Test
    void leadingDotFloatZero() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of(".0 == 0.0"),
                Exp.eq(Exp.val(0.0), Exp.val(0.0)));
    }

    @Test
    void leadingDotFloatNegative() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("-.37 == -0.37"),
                Exp.eq(Exp.val(-0.37), Exp.val(-0.37)));
    }

    @Test
    void leadingDotFloatPlusSign() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("+.37 == 0.37"),
                Exp.eq(Exp.val(0.37), Exp.val(0.37)));
    }

    @Test
    void leadingDotFloatInExpression() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(.5 + .5) == 1.0"),
                Exp.eq(Exp.add(Exp.val(0.5), Exp.val(0.5)), Exp.val(1.0)));
    }

    // ==================== FLOAT: multiple-dot (invalid) ====================

    @Test
    void doubleDotFloat() {
        // "..37" has an extraneous leading dot before a valid leading-dot float
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("..37 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void doubleDotFloatOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == ..37")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void embeddedDotFloat() {
        // ".3.7" has digits both before and after an extra dot
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of(".3.7 == 0.0")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }

    @Test
    void embeddedDotFloatOnRight() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("0.0 == .3.7")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Invalid float literal");
    }
}
