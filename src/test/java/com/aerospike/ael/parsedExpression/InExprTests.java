package com.aerospike.ael.parsedExpression;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.client.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.aerospike.ael.util.TestUtils.NAMESPACE;
import static com.aerospike.ael.util.TestUtils.parseAelExpressionAndCompare;

class InExprTests {

    // --- Single IN + comparison with indexes — IN always excluded from Filter ---

    @Test
    void inAndEq_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void eqAndIn_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.equal("intBin1", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin2"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 == 100 and $.intBin2 in [1, 2, 3]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void eqAndInAndLt_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin3", Long.MIN_VALUE, 49);
        Exp exp = Exp.and(
                Exp.eq(Exp.intBin("intBin1"), Exp.val(100)),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(1, 2, 3))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 == 100 and $.intBin2 in [1, 2, 3] and $.intBin3 < 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inAndGt_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(10, 20)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [10, 20] and $.intBin2 > 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void gtAndIn_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin1", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin2"), Exp.val(List.of(10, 20)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 > 100 and $.intBin2 in [10, 20]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- Two IN parts with indexes — never produce Filter ---

    @Test
    void twoIns_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoIns_noIndexes() {
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4]"),
                filter, exp);
    }

    @Test
    void twoInsAndEq_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin3", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4] and $.intBin3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void eqAndTwoIns_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.equal("intBin1", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin3"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 == 100 and $.intBin2 in [1, 2] and $.intBin3 in [3, 4]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInsAndLtAndGt_allIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("b3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("b4").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.range("b2", Long.MIN_VALUE, 49);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b3"), Exp.val(List.of(2))),
                Exp.gt(Exp.intBin("b4"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 < 50 and $.b3 in [2] and $.b4 > 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- IN bin has highest cardinality — fallback to next best ---

    @Test
    void inBinHighestCard_fallback() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inBinHighestCard_fallbackGt() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin2", 101, Long.MAX_VALUE);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 > 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inBinHighestCard_3exprs() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(5).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.lt(Exp.intBin("intBin3"), Exp.val(50)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 < 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void inBinOnlyIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- Two IN bins with highest cardinality — fallback ---

    @Test
    void twoInBinsHighestCard() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("b3", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b2"), Exp.val(List.of(2))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 in [2] and $.b3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInBinsHighestCard_noOther() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b2"), Exp.val(List.of(2))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 in [2]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInBinsHighCard_withGtLt() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(5).build(),
                Index.builder().namespace(NAMESPACE).bin("b3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("b4").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("b2", 51, Long.MAX_VALUE);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b3"), Exp.val(List.of(2))),
                Exp.lt(Exp.intBin("b4"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 > 50 and $.b3 in [2] and $.b4 < 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    @Test
    void twoInsOnlyIndexed() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("b1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("b2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b1"), Exp.val(List.of(1))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("b2"), Exp.val(List.of(2))),
                Exp.eq(Exp.intBin("b3"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.b1 in [1] and $.b2 in [2] and $.b3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes));
    }

    // --- Index Name Hint: IN + comparison ---

    @Test
    void inAndEq_indexHint_selectsEq() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin2"));
    }

    @Test
    void inAndEq_indexHint_onInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin1"));
    }

    @Test
    void inAndEqAndGt_indexHint_overrides() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin3", 51, Long.MAX_VALUE);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 > 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin3"));
    }

    @Test
    void twoInsAndEq_indexHint() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin3", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4] and $.intBin3 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin3"));
    }

    @Test
    void twoIns_indexHint_noFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4]"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin1"));
    }

    @Test
    void inAndEq_indexHint_unavailable() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "nonExistent"));
    }

    @Test
    void inAndEq_indexHint_nsMismatch() {
        List<Index> indexes = List.of(
                Index.builder().namespace("other_ns").name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin1"));
    }

    @Test
    void inAndEq_indexHint_null() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, null));
    }

    @Test
    void inAndEq_indexHint_empty() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, ""));
    }

    @Test
    void inAndEq_indexHint_overridesAlpha() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(100).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(100).build()
        );
        Filter filter = Filter.range("intBin3", 51, Long.MAX_VALUE);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 > 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin3"));
    }

    // --- Index Name Hint: 3 sub-expressions with hint on IN bin ---

    @Test
    void inAndEqAndGt_indexHint_onInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(50)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 > 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin1"));
    }

    @Test
    void twoInsAndLt_indexHint_onInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin3", Long.MIN_VALUE, 49);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4] and $.intBin3 < 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin1"));
    }

    // --- Index Name Hint: OR expressions ---

    @Test
    void inOrEq_indexHint_noFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] or $.intBin2 == 100"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin2"));
    }

    @Test
    void orInAndGt_indexHint_onOrInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).name("idx_intBin1").bin("intBin1")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin2").bin("intBin2")
                        .indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).name("idx_intBin3").bin("intBin3")
                        .indexType(IndexType.NUMERIC).binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin3", 51, Long.MAX_VALUE);
        Exp exp = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("($.intBin1 in [1, 2] or $.intBin2 == 100) and $.intBin3 > 50"),
                filter, exp, IndexContext.of(NAMESPACE, indexes, "idx_intBin1"));
    }

    // --- Bin Name Hint: IN + comparison ---

    @Test
    void inAndEq_binHint_selectsEq() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin2"));
    }

    @Test
    void inAndEq_binHint_onInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void inAndEqAndGt_binHint_overrides() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(10).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin3", 51, Long.MAX_VALUE);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 > 50"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin3"));
    }

    @Test
    void twoInsAndEq_binHint() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin3", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4] and $.intBin3 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin3"));
    }

    @Test
    void twoIns_binHint_noFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4]"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void inAndEq_binHint_noMatch() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "nonExistent"));
    }

    @Test
    void inAndEq_binHint_null() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, null));
    }

    @Test
    void inAndEq_binHint_nsMismatch() {
        List<Index> indexes = List.of(
                Index.builder().namespace("other_ns").bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = ListExp.getByValue(ListReturnType.EXISTS,
                Exp.intBin("intBin1"), Exp.val(List.of(1, 2, 3)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2, 3] and $.intBin2 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void inAndEq_binHint_overridesAlpha() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(100).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(100).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(100).build()
        );
        Filter filter = Filter.range("intBin3", 51, Long.MAX_VALUE);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 > 50"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin3"));
    }

    // --- Bin Name Hint: 3 sub-expressions with hint on IN bin ---

    @Test
    void inAndEqAndGt_binHint_onInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.equal("intBin2", 100);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.gt(Exp.intBin("intBin3"), Exp.val(50)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 == 100 and $.intBin3 > 50"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin1"));
    }

    @Test
    void twoInsAndLt_binHint_onInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = Filter.range("intBin3", Long.MIN_VALUE, 49);
        Exp exp = Exp.and(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin2"), Exp.val(List.of(3, 4))));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] and $.intBin2 in [3, 4] and $.intBin3 < 50"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin1"));
    }

    // --- Bin Name Hint: OR expressions ---

    @Test
    void inOrEq_binHint_noFilter() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build()
        );
        Filter filter = null;
        Exp exp = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("$.intBin1 in [1, 2] or $.intBin2 == 100"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin2"));
    }

    @Test
    void orInAndGt_binHint_onOrInBin() {
        List<Index> indexes = List.of(
                Index.builder().namespace(NAMESPACE).bin("intBin1").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin2").indexType(IndexType.NUMERIC)
                        .binValuesRatio(1).build(),
                Index.builder().namespace(NAMESPACE).bin("intBin3").indexType(IndexType.NUMERIC)
                        .binValuesRatio(0).build()
        );
        Filter filter = Filter.range("intBin3", 51, Long.MAX_VALUE);
        Exp exp = Exp.or(
                ListExp.getByValue(ListReturnType.EXISTS,
                        Exp.intBin("intBin1"), Exp.val(List.of(1, 2))),
                Exp.eq(Exp.intBin("intBin2"), Exp.val(100)));
        parseAelExpressionAndCompare(
                ExpressionContext.of("($.intBin1 in [1, 2] or $.intBin2 == 100) and $.intBin3 > 50"),
                filter, exp, IndexContext.withBinHint(NAMESPACE, indexes, "intBin1"));
    }
}
