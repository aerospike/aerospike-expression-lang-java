package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.exp.Exp;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static com.aerospike.ael.util.TestUtils.parseFilterExpressionAndCompare;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinExpressionsTests {

    @Test
    void binGT() {
        parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 > 100"), Exp.gt(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 > 'text'"), Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 > \"text\""), Exp.gt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 > '?0'"), Exp.gt(Exp.stringBin("stringBin1"), Exp.val("?0")));

        parseFilterExpressionAndCompare(ExpressionContext.of("100 < $.intBin1"), Exp.lt(Exp.val(100), Exp.intBin("intBin1")));
        parseFilterExpressionAndCompare(ExpressionContext.of("'text' < $.stringBin1"), Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
        parseFilterExpressionAndCompare(ExpressionContext.of("\"text\" < $.stringBin1"), Exp.lt(Exp.val("text"), Exp.stringBin("stringBin1")));
    }

    @Test
    void binGE() {
        parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 >= 100"), Exp.ge(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 >= 'text'"), Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 >= \"text\""), Exp.ge(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLT() {
        parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 < 100"), Exp.lt(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 < 'text'"), Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 < \"text\""), Exp.lt(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binLE() {
        parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 <= 100"), Exp.le(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 <= 'text'"), Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.stringBin1 <= \"text\""), Exp.le(Exp.stringBin("stringBin1"), Exp.val("text")));
    }

    @Test
    void binEquals() {
        parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 == 100"), Exp.eq(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.strBin == \"yes\""), Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.strBin == 'yes'"), Exp.eq(Exp.stringBin("strBin"), Exp.val("yes")));

        parseFilterExpressionAndCompare(ExpressionContext.of("100 == $.intBin1"), Exp.eq(Exp.val(100), Exp.intBin("intBin1")));
        parseFilterExpressionAndCompare(ExpressionContext.of("\"yes\" == $.strBin"), Exp.eq(Exp.val("yes"), Exp.stringBin("strBin")));
        parseFilterExpressionAndCompare(ExpressionContext.of("'yes' == $.strBin"), Exp.eq(Exp.val("yes"), Exp.stringBin("strBin")));
    }

    @Test
    void binNotEquals() {
        parseFilterExpressionAndCompare(ExpressionContext.of("$.intBin1 != 100"), Exp.ne(Exp.intBin("intBin1"), Exp.val(100)));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.strBin != \"yes\""), Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
        parseFilterExpressionAndCompare(ExpressionContext.of("$.strBin != 'yes'"), Exp.ne(Exp.stringBin("strBin"), Exp.val("yes")));
    }

    @Test
    void negativeStringBinEquals() {
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.strBin == yes")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse given AEL expression input")
                .hasMessageContaining("[Parser] mismatched input '<EOF>'")
                .hasMessageContaining("at character 15");
    }

    // TODO: Will be handled in FMWK-486
    //@Test
    //void logicalOperators_functionCalls() {
    //    translate("$.a.exists() and $.b.exists()");
    //}
}
