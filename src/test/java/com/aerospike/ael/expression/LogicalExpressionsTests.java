package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogicalExpressionsTests {

    @Test
    void binLogicalAndOrCombinations() {
        Exp expected1 = Exp.and(Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.gt(Exp.intBin("intBin2"), Exp.val(100)));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100"), expected1);

        Exp expected2 = Exp.or(
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100))
                ),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
        );
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 or $.intBin3 < 100"),
                expected2);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100"),
                expected2);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("(($.intBin1 > 100 and $.intBin2 > 100) or $.intBin3 < 100)"),
                expected2);

        Exp expected3 = Exp.and(
                Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                Exp.or(
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100))
                )
        );
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100))"),
                expected3);
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100 and ($.intBin2 > 100 or $.intBin3 < 100)"),
                expected3);
        // Check that parentheses make difference
        assertThatThrownBy(
                () -> TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("($.intBin1 > 100 and" +
                        " ($.intBin2 > 100 or $.intBin3 < 100))"), expected2)
        ).isInstanceOf(AssertionFailedError.class)
                .hasMessageContaining("expected:");
    }

    @Test
    void logicalNot() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("not($.keyExists())"), Exp.not(Exp.keyExists()));
    }

    @Test
    void binLogicalExclusive() {
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("exclusive($.hand == \"hook\", $.leg == \"peg\")"),
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")),
                        Exp.eq(Exp.stringBin("leg"), Exp.val("peg"))));
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("exclusive($.hand == 'hook', $.leg == 'peg')"),
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")),
                        Exp.eq(Exp.stringBin("leg"), Exp.val("peg"))));

        // More than 2 expressions exclusive
        TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("exclusive($.a == \"aVal\", $.b == \"bVal\", " +
                        "$.c == \"cVal\", $.d == 4)"),
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("a"), Exp.val("aVal")),
                        Exp.eq(Exp.stringBin("b"), Exp.val("bVal")),
                        Exp.eq(Exp.stringBin("c"), Exp.val("cVal")),
                        Exp.eq(Exp.intBin("d"), Exp.val(4))));
    }

    @Test
    void flatHierarchyAnd() {
        TestUtils.parseFilterExpressionAndCompare(
                ExpressionContext.of("$.intBin1 > 100 and $.intBin2 > 100 and $.intBin3 < 100 and $.intBin4 < 100"),
                Exp.and(
                        Exp.gt(Exp.intBin("intBin1"), Exp.val(100)),
                        Exp.gt(Exp.intBin("intBin2"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin3"), Exp.val(100)),
                        Exp.lt(Exp.intBin("intBin4"), Exp.val(100))
                )
        );
    }

    @Test
    void negativeSyntaxLogicalOperators() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("($.intBin1 > 100 and ($.intBin2 > 100) or")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 41");

        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("and ($.intBin1 > 100 and ($.intBin2 > 100)")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] extraneous input 'and'")
                .hasMessageContaining("at character 0");

        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("($.intBin1 > 100 and ($.intBin2 > 100) not")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 39");

        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("($.intBin1 > 100 and ($.intBin2 > 100) exclusive")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 39");
    }

    @Test
    void negativeBinLogicalExclusiveWithOneParam() {
        assertThatThrownBy(() -> TestUtils.parseFilterExpressionAndCompare(ExpressionContext.of("exclusive($.hand == \"hook\")"),
                Exp.exclusive(
                        Exp.eq(Exp.stringBin("hand"), Exp.val("hook")))))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] no viable alternative at input")
                .hasMessageContaining("at character 26");
    }
}
