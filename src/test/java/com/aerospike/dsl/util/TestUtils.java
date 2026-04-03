package com.aerospike.dsl.util;

import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.Expression;
import com.aerospike.dsl.client.query.Filter;
import com.aerospike.dsl.impl.DSLParserImpl;
import lombok.experimental.UtilityClass;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UtilityClass
public class TestUtils {

    public static final String NAMESPACE = "test1";
    private static final DSLParserImpl parser = new DSLParserImpl();

    /**
     * Parses the given DSL expression and extracts the resulting {@link Exp} object.
     *
     * @param expressionContext The input representing DSL expression
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static Exp parseFilterExp(ExpressionContext expressionContext) {
        return parser.parseExpression(expressionContext).getResult().getExp();
    }

    /**
     * Parses the given DSL expression and returns the resulting {@link ParsedExpression} object.
     *
     * @param expressionContext The {@link ExpressionContext} representing DSL expression
     * @param indexContext      The {@link IndexContext} to be used for building secondary index filter
     * @return The {@link Exp} object derived from the parsed filter expression
     */
    public static ParsedExpression getParsedExpression(ExpressionContext expressionContext, IndexContext indexContext) {
        return parser.parseExpression(expressionContext, indexContext);
    }

    /**
     * Parses the given DSL expression, extracts the resulting {@link Exp} object, converts it to an {@link Expression}
     * object, and then asserts that it is equal to the {@code expected} {@link Exp} also built into an
     * {@link Expression}.
     *
     * @param expressionContext The input representing DSL expression
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
     * @param expressionContext The input representing DSL expression
     * @param indexContext      The {@link IndexContext} to be used for building secondary index filter
     * @return A {@link Filter} object derived from the parsed result
     */
    public static Filter parseFilter(ExpressionContext expressionContext, IndexContext indexContext) {
        return parser.parseExpression(expressionContext, indexContext).getResult().getFilter();
    }

    /**
     * Parses the given DSL expression and asserts that the result is equal to the {@code expected} {@link Filter}
     * object.
     *
     * @param input    The input representing DSL expression
     * @param expected The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(ExpressionContext input, Filter expected) {
        Filter actualFilter = parseFilter(input);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given DSL expression using the provided {@link IndexContext} and asserts that the result is equal to
     * the {@code expected} {@link Filter} object.
     *
     * @param input        The string input representing DSL expression
     * @param indexContext The {@link IndexContext} to be used for building secondary index filter
     * @param expected     The expected {@link Filter} object to compare against the parsed result
     */
    public static void parseFilterAndCompare(ExpressionContext input, IndexContext indexContext, Filter expected) {
        Filter actualFilter = parseFilter(input, indexContext);
        assertEquals(expected, actualFilter);
    }

    /**
     * Parses the given DSL expression and compares the resulting
     * {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param expressionContext The input representing DSL expression
     * @param filter            The expected {@link Filter} component of the parsed result
     * @param exp               The expected {@link Exp} component of the parsed result. Can be {@code null}
     */
    public static void parseDslExpressionAndCompare(ExpressionContext expressionContext, Filter filter, Exp exp) {
        ParsedExpression actualExpression = parser.parseExpression(expressionContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }

    /**
     * Parses the given DSL expression using the provided {@link IndexContext}
     * and compares the resulting {@link Filter} and {@link Exp} components with the expected {@code filter} and {@code exp}.
     *
     * @param expressionContext The input representing DSL expression
     * @param filter            The expected {@link Filter} component of the parsed result
     * @param exp               The expected {@link Exp} component of the parsed result. Can be {@code null}
     * @param indexContext      The {@link IndexContext} to be used for building secondary index filter
     */
    public static void parseDslExpressionAndCompare(ExpressionContext expressionContext, Filter filter, Exp exp, IndexContext indexContext) {
        ParsedExpression actualExpression = parser.parseExpression(expressionContext, indexContext);
        assertEquals(filter, actualExpression.getResult().getFilter());
        Exp actualExp = actualExpression.getResult().getExp();
        assertEquals(exp == null ? null : Exp.build(exp), actualExp == null ? null : Exp.build(actualExp));
    }

    /**
     * Parses two DSL expression strings and asserts that they produce identical packed {@link Expression} bytes.
     *
     * @param dslActual   The DSL string whose result is being verified
     * @param dslExpected The reference DSL string that defines the expected result
     */
    public static void parseDslAndCompare(String dslActual, String dslExpected) {
        Expression actual = Exp.build(parser.parseExpression(ExpressionContext.of(dslActual)).getResult().getExp());
        Expression expected = Exp.build(parser.parseExpression(ExpressionContext.of(dslExpected)).getResult().getExp());
        assertEquals(expected, actual);
    }

    /**
     * Parses the given DSL path String into array of {@link CTX}.
     *
     * @param pathToCtx String input representing DSL path
     * @return The array of {@link CTX} or null
     */
    public static CTX[] parseCtx(String pathToCtx) {
        return parser.parseCTX(pathToCtx);
    }

    /**
     * Parses the given DSL path String and compares arrays of {@link CTX} using {@link CTX#toBase64(CTX[])} method.
     *
     * @param pathToCtx String input representing DSL path
     * @param expected  The array of {@link CTX} to be used for comparing
     */
    public static void parseCtxAndCompareAsBase64(String pathToCtx, CTX[] expected) {
        CTX[] actualCtx = parser.parseCTX(pathToCtx);
        assertEquals(expected == null ? null : CTX.toBase64(expected), actualCtx == null ? null : CTX.toBase64(actualCtx));
    }
}
