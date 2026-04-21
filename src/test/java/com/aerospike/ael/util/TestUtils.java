package com.aerospike.ael.util;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.Expression;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.impl.AelParserImpl;
import lombok.experimental.UtilityClass;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    public static final String NAMESPACE = "test1";
    private static final AelParserImpl parser = new AelParserImpl();

    /**
     * Parses the given AEL expression and extracts the resulting {@link Exp} object.
     *
     * @param expressionContext The input representing AEL expression
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static Exp parseFilterExp(ExpressionContext expressionContext) {
        return parser.parseExpression(expressionContext).getResult().getExp();
    }

    /**
     * Parses the given AEL expression and returns the resulting {@link ParsedExpression} object.
     *
     * @param expressionContext The {@link ExpressionContext} representing AEL expression
     * @param indexContext      The {@link IndexContext} to be used for building secondary index filter
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static ParsedExpression getParsedExpression(ExpressionContext expressionContext, IndexContext indexContext) {
        return parser.parseExpression(expressionContext, indexContext);
    }

    /**
     * Parses the given AEL expression, extracts the resulting {@link Exp} object, converts it to an {@link Expression}
     * object, and then asserts that it is equal to the {@code expected} {@link Exp} also built into an
     * {@link Expression}.
     *
     * @param expressionContext The input representing AEL expression
     * @param expected          The expected {@link Exp} object to compare against the parsed result
     */
    public static void parseFilterExpressionAndCompare(ExpressionContext expressionContext, Exp expected) {
        Expression actualExpression = Exp.build(parser.parseExpression(expressionContext).getResult().getExp());
        Expression expectedExpression = Exp.build(expected);
        assertEquals(expectedExpression, actualExpression);
    }

    /**
     * Parses the given DL expression using the provided {@link ExpressionContext} to match placeholders
     * and returns the resulting {@link Filter} object.
     *
     * @param expressionContext The {@link ExpressionContext} to be used to match placeholders
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(ExpressionContext expressionContext) {
        return parser.parseExpression(expressionContext).getResult().getFilter();
    }

    /**
     * Parses the given DL expression using the provided {@link IndexContext} and returns the resulting {@link Filter} object.
     *
     * @param expressionContext The input representing AEL expression
     * @param indexContext      The {@link IndexContext} to be used for building secondary index filter
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(ExpressionContext expressionContext, IndexContext indexContext) {
        return parser.parseExpression(expressionContext, indexContext).getResult().getFilter();
    }

    /**
     * Parses the given AEL expression and asserts that the result is equal to the {@code expected} {@link Filter}
     * object.
     *
     * @param input    The input representing AEL expression
     * @param expected The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(ExpressionContext input, Filter expected) {
        Filter actualFilter = parseFilter(input);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given AEL expression using the provided {@link IndexContext} and asserts that the result is equal to
     * the {@code expected} {@link Filter} object.
     *
     * @param input        The string input representing AEL expression
     * @param indexContext The {@link IndexContext} to be used for building secondary index filter
     * @param expected     The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(ExpressionContext input, IndexContext indexContext, Filter expected) {
        Filter actualFilter = parseFilter(input, indexContext);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given AEL expression and compares the resulting
     * {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param expressionContext The input representing AEL expression
     * @param filter            The expected {@link Filter} component of the parsed result
     * @param exp               The expected {@link Exp} component of the parsed result. Can be {@code null}
     */
    public static void parseAelExpressionAndCompare(ExpressionContext expressionContext, Filter filter, Exp exp) {
        ParsedExpression actualExpression = parser.parseExpression(expressionContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }

    /**
     * Parses the given AEL expression using the provided {@link IndexContext}
     * and compares the resulting {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param expressionContext The input representing AEL expression
     * @param filter            The expected {@link Filter} component of the parsed result
     * @param exp               The expected {@link Exp} component of the parsed result. Can be {@code null}
     * @param indexContext      The {@link IndexContext} to be used for building secondary index filter
     */
    public static void parseAelExpressionAndCompare(ExpressionContext expressionContext, Filter filter, Exp exp, IndexContext indexContext) {
        ParsedExpression actualExpression = parser.parseExpression(expressionContext, indexContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }

    /**
     * Parses two AEL expression strings and asserts that they produce identical packed {@link Expression} bytes.
     *
     * @param aelActual   The AEL string whose result is being verified
     * @param aelExpected The reference AEL string that defines the expected result
     */
    public static void parseAelAndCompare(String aelActual, String aelExpected) {
        Expression actual = Exp.build(parser.parseExpression(ExpressionContext.of(aelActual)).getResult().getExp());
        Expression expected = Exp.build(parser.parseExpression(ExpressionContext.of(aelExpected)).getResult().getExp());
        assertEquals(expected, actual);
    }

    /**
     * Parses the given AEL path String into array of {@link CTX}.
     *
     * @param pathToCtx String input representing AEL path
     * @return The array of {@link CTX} or null
     */
    public static CTX[] parseCtx(String pathToCtx) {
        return parser.parseCTX(pathToCtx);
    }

    /**
     * Parses the given AEL path String and compares arrays of {@link CTX} using {@link CTX#toBase64(CTX[])} method.
     *
     * @param pathToCtx String input representing AEL path
     * @param expected  The array of {@link CTX} to be used for comparing
     */
    public static void parseCtxAndCompareAsBase64(String pathToCtx, CTX[] expected) {
        CTX[] actualCtx = parser.parseCTX(pathToCtx);
        assertEquals(expected == null ? null : CTX.toBase64(expected), actualCtx == null ? null : CTX.toBase64(actualCtx));
    }
}
