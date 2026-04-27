package com.aerospike.ael.expression;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.util.TestUtils;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseFilterExp;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordMetadataTests {

    @Test
    void deviceSize() {
        // Expression to find records that occupy more than 1 MiB of storage space
        ExpressionContext input = ExpressionContext.of("$.deviceSize() > 1048576");
        Exp expected = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void memorySize() {
        // Expression to find records that occupy more than 1 MiB of memory
        ExpressionContext input = ExpressionContext.of("$.memorySize() > 1048576");
        Exp expected = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void recordSize() {
        // Expression to find records that occupy more than 1 MiB of memory
        ExpressionContext input = ExpressionContext.of("$.recordSize() > 1048576");
        Exp expected = Exp.gt(Exp.recordSize(), Exp.val(1024 * 1024));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void digestModulo() {
        // Expression to find records where digest mod 3 equals 0
        ExpressionContext input = ExpressionContext.of("$.digestModulo(3) == 0");
        Exp expected = Exp.eq(Exp.digestModulo(3), Exp.val(0));
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // Expression to find records where digest mod 3 equals the value stored in the bin called "digestModulo"
        ExpressionContext input2 = ExpressionContext.of("$.digestModulo(3) == $.digestModulo");
        Exp expected2 = Exp.eq(Exp.digestModulo(3), Exp.intBin("digestModulo"));
        TestUtils.parseFilterExpressionAndCompare(input2, expected2);
    }

    @Test
    void isTombstone() {
        // Expression to find records that are tombstones
        ExpressionContext input = ExpressionContext.of("$.isTombstone()");
        Exp expected = Exp.isTombstone();
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void keyExists() {
        // Expression to find records that has a stored key
        ExpressionContext input = ExpressionContext.of("$.keyExists()");
        Exp expected = Exp.keyExists();
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    // Comparing Metadata to a Bin
    @Test
    void lastUpdate() {
        // Expression to find records where the last-update-time is less than bin 'updateBy'
        ExpressionContext inputMetadataLeft = ExpressionContext.of("$.lastUpdate() < $.updateBy");
        Exp expectedLeft = Exp.lt(Exp.lastUpdate(), Exp.intBin("updateBy"));
        TestUtils.parseFilterExpressionAndCompare(inputMetadataLeft, expectedLeft);

        // Expression to find records where the last-update-time is less than bin 'updateBy'
        ExpressionContext inputMetadataRight = ExpressionContext.of("$.updateBy > $.lastUpdate()");
        Exp expectedRight = Exp.gt(Exp.intBin("updateBy"), Exp.lastUpdate());
        TestUtils.parseFilterExpressionAndCompare(inputMetadataRight, expectedRight);
    }

    @Test
    void sinceUpdate() {
        // Expression to find records that were updated within the last 2 hours
        ExpressionContext input = ExpressionContext.of("$.sinceUpdate() < 7200000");
        Exp expected = Exp.lt(Exp.sinceUpdate(), Exp.val(2 * 60 * 60 * 1000));
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // Expression to find records that were update within the value stored in the bin called "intBin"
        ExpressionContext input2 = ExpressionContext.of("$.sinceUpdate() < $.intBin");
        Exp expected2 = Exp.lt(Exp.sinceUpdate(), Exp.intBin("intBin"));
        TestUtils.parseFilterExpressionAndCompare(input2, expected2);

        // Expression to find records that were updated within the value stored in the bin called "sinceUpdate"
        ExpressionContext input3 = ExpressionContext.of("$.sinceUpdate() < $.sinceUpdate");
        Exp expected3 = Exp.lt(Exp.sinceUpdate(), Exp.intBin("sinceUpdate"));
        TestUtils.parseFilterExpressionAndCompare(input3, expected3);

        // Expression to find records that were updated within the value stored in the bin called "sinceUpdate"
        ExpressionContext input4 = ExpressionContext.of("$.sinceUpdate > $.sinceUpdate()");
        Exp expected4 = Exp.gt(Exp.intBin("sinceUpdate"), Exp.sinceUpdate());
        TestUtils.parseFilterExpressionAndCompare(input4, expected4);
    }

    @Test
    void setName() {
        // Expression to find records where the set_name is either 'groupA' or 'groupB'
        ExpressionContext input = ExpressionContext.of("$.setName() == \"groupA\" or $.setName() == \"groupB\"");
        Exp expected = Exp.or(
                Exp.eq(Exp.setName(), Exp.val("groupA")),
                Exp.eq(Exp.setName(), Exp.val("groupB"))
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // set name compared with String Bin
        ExpressionContext input2 = ExpressionContext.of("$.mySetBin == $.setName()");
        expected = Exp.eq(Exp.stringBin("mySetBin"), Exp.setName());
        TestUtils.parseFilterExpressionAndCompare(input2, expected);
    }

    @Test
    void ttl() {
        // Expression to find records that will expire within 24 hours
        ExpressionContext input = ExpressionContext.of("$.ttl() <= 86400");
        Exp expected = Exp.le(Exp.ttl(), Exp.val(24 * 60 * 60));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    //@Test
    void negativeTtlAsDifferentType() {
        // TODO: should be supported when adding operator + metadata validations (requires a refactor)
        assertThatThrownBy(() -> parseFilterExp(ExpressionContext.of("$.ttl() == true")))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Expecting non-bin operand, got BOOL_OPERAND");
    }

    @Test
    void voidTime() {
        // Expression to find records where the void-time is set to 'never expire'
        ExpressionContext input = ExpressionContext.of("$.voidTime() == -1");
        Exp expected = Exp.eq(Exp.voidTime(), Exp.val(-1));
        TestUtils.parseFilterExpressionAndCompare(input, expected);
    }

    @Test
    void metadataWithLogicalOperatorsExpressions() {
        // test AND
        ExpressionContext input = ExpressionContext.of("$.deviceSize() > 1024 and $.ttl() < 300");
        Exp expected = Exp.and(
                Exp.gt(Exp.recordSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        // test OR
        ExpressionContext input2 = ExpressionContext.of("$.deviceSize() > 1024 or $.ttl() < 300");
        Exp expected2 = Exp.or(
                Exp.gt(Exp.recordSize(), Exp.val(1024)),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        TestUtils.parseFilterExpressionAndCompare(input2, expected2);
    }

    @Test
    void metadataAsExpressionWithLogicalOperator() {
        ExpressionContext input = ExpressionContext.of("$.isTombstone() and $.ttl() < 300");
        Exp expected = Exp.and(
                Exp.isTombstone(),
                Exp.lt(Exp.ttl(), Exp.val(300))
        );
        TestUtils.parseFilterExpressionAndCompare(input, expected);

        ExpressionContext input2 = ExpressionContext.of("$.ttl() < 300 or $.keyExists()");
        expected = Exp.or(
                Exp.lt(Exp.ttl(), Exp.val(300)),
                Exp.keyExists()
        );
        TestUtils.parseFilterExpressionAndCompare(input2, expected);
    }
}
