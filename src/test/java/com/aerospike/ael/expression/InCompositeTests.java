package com.aerospike.ael.expression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;

class InCompositeTests {

    @Test
    void inWithAndOperator() {
        Exp expected = Exp.and(
                Exp.gt(Exp.intBin("cost"), Exp.val(50)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("status"), Exp.val(List.of("active", "pending"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.cost > 50 and $.status in [\"active\", \"pending\"]"), expected);
    }

    @Test
    void inWithOrOperator() {
        Exp expected = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("status"), Exp.val(List.of("active"))),
                Exp.gt(Exp.intBin("priority"), Exp.val(5)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.status in [\"active\"] or $.priority > 5"), expected);
    }

    @Test
    void complexExpressionWithIn() {
        Exp expected = Exp.and(
                Exp.gt(Exp.intBin("cost"), Exp.val(50)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("status"), Exp.listBin("allowedStatuses")),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.val("available"), Exp.listBin("bookableStates")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.cost > 50 and $.status.get(type: INT) in $.allowedStatuses" +
                        " and \"available\" in $.bookableStates"), expected);
    }

    @Test
    void inWithParentheses() {
        Exp expected = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.gt(Exp.intBin("age"), Exp.val(18)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("($.name in [\"Bob\"]) and $.age > 18"), expected);
    }

    @Test
    void inInsideLetStructure() {
        Exp expected = Exp.let(
                Exp.def("allowed", Exp.val(List.of("Bob", "Mary"))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.var("allowed")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(allowed = [\"Bob\", \"Mary\"])" +
                        " then ($.name.get(type: STRING) in ${allowed})"), expected);
    }

    @Test
    void inInsideWhenCondition() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.val("VIP"),
                Exp.val("regular"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.name.get(type: STRING) in [\"Bob\"] => \"VIP\"," +
                        " default => \"regular\")"), expected);
    }

    @Test
    void notWrappingIn() {
        Exp expected = Exp.not(ListExp.getByValue(ListReturnType.EXISTS,
                Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("not($.name in [\"Bob\", \"Mary\"])"), expected);
    }

    @Test
    void nestedLetOuterListVar() {
        Exp expected = Exp.let(
                Exp.def("x", Exp.val(List.of("a", "b"))),
                Exp.let(
                        Exp.def("y", Exp.val(3)),
                        ListExp.getByValue(ListReturnType.EXISTS,
                                Exp.stringBin("name"), Exp.var("x"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(x = [\"a\", \"b\"]) then " +
                        "(let(y = 3) then ($.name.get(type: STRING) in ${x}))"), expected);
    }

    @Test
    void nestedLetShadowedVar() {
        Exp expected = Exp.let(
                Exp.def("x", Exp.val(1)),
                Exp.let(
                        Exp.def("x", Exp.val(List.of("a"))),
                        ListExp.getByValue(ListReturnType.EXISTS,
                                Exp.stringBin("name"), Exp.var("x"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(x = 1) then " +
                        "(let(x = [\"a\"]) then ($.name.get(type: STRING) in ${x}))"), expected);
    }

    @Test
    void nestedLetVarBoundToVar() {
        Exp expected = Exp.let(
                Exp.def("x", Exp.val(List.of(1, 2))),
                Exp.let(
                        Exp.def("y", Exp.var("x")),
                        ListExp.getByValue(ListReturnType.EXISTS,
                                Exp.val(1), Exp.var("y"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(x = [1, 2]) then " +
                        "(let(y = ${x}) then (1 in ${y}))"), expected);
    }

    // Known limitation: transitive variable indirection is not resolved statically.
    // y -> ${x} where x = 1 (scalar) — the analysis conservatively allows this
    // because it cannot follow variable-to-variable bindings.
    @Test
    void transitiveVarIndirection() {
        Exp expected = Exp.let(
                Exp.def("x", Exp.val(1)),
                Exp.let(
                        Exp.def("y", Exp.var("x")),
                        ListExp.getByValue(ListReturnType.EXISTS,
                                Exp.val("foo"), Exp.var("y"))));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(x = 1) then " +
                        "(let(y = ${x}) then (\"foo\" in ${y}))"), expected);
    }

    // Known limitation: WHEN_STRUCTURE return type is not analyzed branch-by-branch,
    // so a WHEN that always returns a scalar is conservatively allowed as right operand of IN.
    @Test
    void whenScalarBranchesAllowedConservatively() {
        Exp expected = Exp.let(
                Exp.def("x", Exp.cond(
                        Exp.val(true), Exp.val(1),
                        Exp.val(2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.val("foo"), Exp.var("x")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(x = when(true => 1, default => 2))" +
                        " then (\"foo\" in ${x})"), expected);
    }

    @Test
    void arithmeticExprAsLeftIn() {
        Exp expected = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.add(Exp.intBin("a"), Exp.val(5)),
                Exp.val(List.of(10, 20, 30)));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.a + 5 in [10, 20, 30]"), expected);
    }

    @Test
    void inWithIntTypeInWhenCond() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("age"), Exp.val(List.of(18, 21))),
                Exp.val("eligible"),
                Exp.val("ineligible"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.age.get(type: INT) in [18, 21] => \"eligible\"," +
                        " default => \"ineligible\")"), expected);
    }

    @Test
    void multipleInConditionsInWhen() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("role"), Exp.val(List.of("admin"))),
                Exp.val(1),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("role"), Exp.val(List.of("user"))),
                Exp.val(2),
                Exp.val(0));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.role in [\"admin\"] => 1," +
                        " $.role in [\"user\"] => 2," +
                        " default => 0)"), expected);
    }

    @Test
    void mixedInAndComparisonInWhen() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob", "Mary"))),
                Exp.val("known"),
                Exp.gt(Exp.intBin("age"), Exp.val(65)),
                Exp.val("senior"),
                Exp.val("other"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.name in [\"Bob\", \"Mary\"] => \"known\"," +
                        " $.age > 65 => \"senior\"," +
                        " default => \"other\")"), expected);
    }

    @Test
    void inWithBinRightInWhenCond() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("status"), Exp.listBin("allowedStatuses")),
                Exp.val("ok"),
                Exp.val("rejected"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.status.get(type: INT) in $.allowedStatuses" +
                        " => \"ok\", default => \"rejected\")"), expected);
    }

    @Test
    void whenResultWithInCondition() {
        Exp expected = Exp.eq(
                Exp.stringBin("label"),
                Exp.cond(
                        ListExp.getByValue(ListReturnType.EXISTS,
                                Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                        Exp.val("VIP"),
                        Exp.val("regular")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("$.label.get(type: STRING) == " +
                        "(when($.name in [\"Bob\"] => \"VIP\", default => \"regular\"))"),
                expected);
    }

    @Test
    void inInsideWhenWithLetVariable() {
        Exp expected = Exp.let(
                Exp.def("allowed", Exp.val(List.of("Bob", "Mary"))),
                Exp.cond(
                        ListExp.getByValue(ListReturnType.EXISTS,
                                Exp.stringBin("name"), Exp.var("allowed")),
                        Exp.val("found"),
                        Exp.val("missing")));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("let(allowed = [\"Bob\", \"Mary\"]) then " +
                        "(when($.name.get(type: STRING) in ${allowed} => \"found\"," +
                        " default => \"missing\"))"), expected);
    }

    @Test
    void inInsideWhenWithPlaceholder() {
        Exp expected = Exp.cond(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.stringBin("name"), Exp.val(List.of("Bob"))),
                Exp.val("match"),
                Exp.val("no match"));
        parseFilterExpressionAndCompare(
                ExpressionContext.of("when($.name.get(type: STRING) in ?0 => \"match\"," +
                        " default => \"no match\")",
                        PlaceholderValues.of(List.of("Bob"))), expected);
    }
}
