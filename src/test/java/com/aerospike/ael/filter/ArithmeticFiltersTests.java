package com.aerospike.ael.filter;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.client.query.IndexType;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static com.aerospike.ael.util.TestUtils.parseFilter;
import static com.aerospike.ael.util.TestUtils.parseFilterAndCompare;
import static org.assertj.core.api.Assertions.assertThat;

public class ArithmeticFiltersTests {

    String NAMESPACE = "test1";
    Collection<Index> INDEXES = List.of(
            Index.builder().namespace("test1").bin("apples").indexType(IndexType.NUMERIC).binValuesRatio(1).build(),
            Index.builder().namespace("test1").bin("bananas").indexType(IndexType.NUMERIC).binValuesRatio(1).build()
    );
    IndexContext INDEX_FILTER_INPUT = IndexContext.of(NAMESPACE, INDEXES);

    @Test
    void add() {
        // not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("($.apples + $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull();

        parseFilterAndCompare(ExpressionContext.of("($.apples + 5) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 - 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples + 5) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 - 5, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples + 5) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 - 5 - 1));
        parseFilterAndCompare(ExpressionContext.of("($.apples + 5) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 - 5));

        parseFilterAndCompare(ExpressionContext.of("(9 + $.bananas) > 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 - 9 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("(9 + $.bananas) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 - 9, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("(9 + $.bananas) < 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 - 9 - 1));
        parseFilterAndCompare(ExpressionContext.of("(9 + $.bananas) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 - 9));

        assertThat(parseFilter(ExpressionContext.of("(5.2 + $.bananas) > 10.2"))).isNull(); // not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("($.apples + $.bananas + 5) > 10"))).isNull(); // not supported by the current grammar
    }

    @Test
    void sub() {
        assertThat(parseFilter(ExpressionContext.of("($.apples - $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull(); // not supported by secondary index filter

        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 + 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 + 5, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 + 5 - 1));
        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 + 5));

        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) > -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -10 + 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) >= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -10 + 5, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) < -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 + 5 - 1));
        parseFilterAndCompare(ExpressionContext.of("($.apples - 5) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 + 5));

        parseFilterAndCompare(ExpressionContext.of("(9 - $.bananas) > 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 9 - 10 - 1));
        parseFilterAndCompare(ExpressionContext.of("(9 - $.bananas) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 9 - 10));
        parseFilterAndCompare(ExpressionContext.of("(9 - $.bananas) < 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 9 - 10 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("(9 - $.bananas) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 9 - 10, Long.MAX_VALUE));

        assertThat(parseFilter(ExpressionContext.of("($.apples - $.bananas) > 10"))).isNull(); // not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("($.apples - $.bananas - 5) > 10"))).isNull(); // not supported by the current grammar
    }

    @Test
    void mul() {
        assertThat(parseFilter(ExpressionContext.of("($.apples * $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull(); // not supported by secondary index filter

        parseFilterAndCompare(ExpressionContext.of("($.apples * 5) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / 5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples * 5) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / 5, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples * 5) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / 5 - 1));
        parseFilterAndCompare(ExpressionContext.of("($.apples * 5) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / 5));

        parseFilterAndCompare(ExpressionContext.of("(9 * $.bananas) > 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 / 9 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("(9 * $.bananas) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 10 / 9, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("(9 * $.bananas) < 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 / 9));
        parseFilterAndCompare(ExpressionContext.of("(9 * $.bananas) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", Long.MIN_VALUE, 10 / 9));

        parseFilterAndCompare(ExpressionContext.of("($.apples * -5) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / -5 - 1));
        parseFilterAndCompare(ExpressionContext.of("($.apples * -5) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 10 / -5));
        parseFilterAndCompare(ExpressionContext.of("($.apples * -5) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / -5 + 1, Long.MAX_VALUE));
        parseFilterAndCompare(ExpressionContext.of("($.apples * -5) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 10 / -5, Long.MAX_VALUE));

        assertThat(parseFilter(ExpressionContext.of("(0 * $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull(); // Cannot divide by zero

        parseFilterAndCompare(ExpressionContext.of("(9 * $.bananas) > 0"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 0 / 9 + 1, Long.MAX_VALUE));

        assertThat(parseFilter(ExpressionContext.of("($.apples * $.bananas - 5) > 10"))).isNull(); // not supported by the current grammar
    }

    @Test
    void div_twoBins() {
        assertThat(parseFilter(ExpressionContext.of("($.apples / $.bananas) <= 10"), INDEX_FILTER_INPUT)).isNull(); // not supported by secondary index filter
    }

    @Test
    void div_binIsDivided_leftNumberIsLarger() {
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * 10 + 1, Long.MAX_VALUE)); // [501, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * 10, Long.MAX_VALUE)); // [500, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * 10 - 1)); // [-2^63, 499]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * 10)); // [-2^63, 500]

        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -50 * 10 - 1)); // [-2^63, -501]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -50 * 10)); // [-2^63, -500]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -50 * 10 + 1, Long.MAX_VALUE)); // [-499, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -50 * 10, Long.MAX_VALUE)); // [-500, 2^63 - 1]

        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) > -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * -10 + 1, Long.MAX_VALUE)); // [-499, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) >= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 50 * -10, Long.MAX_VALUE)); // [-500, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) < -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * -10 - 1)); // [-2^63, -501]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 50) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 50 * -10)); // [-2^63, -500]

        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) > -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -50 * -10 - 1)); // [-2^63, 499]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) >= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 * -50)); // [-2^63, 500]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) < -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -50 + 1, Long.MAX_VALUE)); // [501, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -50) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -50, Long.MAX_VALUE)); // [500, 2^63 - 1]
    }

    @Test
    void div_binIsDivided_leftNumberIsSmaller() {
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 10 + 1, Long.MAX_VALUE)); // [51, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 10, Long.MAX_VALUE)); // [50, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 10 - 1)); // [-2^63, 49]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 10)); // [-2^63, 50]

        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) > 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * 10 - 1)); // [-2^63, -51]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * 10)); // [-2^63, -50]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) < 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * 10 + 1, Long.MAX_VALUE)); // [-49, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) <= 10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * 10, Long.MAX_VALUE)); // [-50, 2^63 - 1]

        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) > -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * -10 + 1, Long.MAX_VALUE)); // [-49, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) >= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * -10, Long.MAX_VALUE)); // [-50, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) < -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * -10 - 1)); // [-2^63, -51]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * -10)); // [-2^63, -50]

        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) > -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * -10 - 1)); // [-2^63, 49]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) >= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -10 * -5)); // [-2^63, 50]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) < -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -5 + 1, Long.MAX_VALUE)); // [51, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("apples", -10 * -5, Long.MAX_VALUE)); // [50, 2^63 - 1]
    }

    @Test
    void div_binIsDivided_leftNumberEqualsRight() {
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) > 5"), INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 5 + 1, Long.MAX_VALUE)); // [26, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) >= 5"), INDEX_FILTER_INPUT,
                Filter.range("apples", 5 * 5, Long.MAX_VALUE)); // [25, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) < 5"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 5 - 1)); // [-2^63, 24]
        parseFilterAndCompare(ExpressionContext.of("($.apples / 5) <= 5"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, 5 * 5)); // [-2^63, 25]

        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) > -5"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * -5 - 1)); // [-2^63, 24]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) >= -5"), INDEX_FILTER_INPUT,
                Filter.range("apples", Long.MIN_VALUE, -5 * -5)); // [-2^63, 25]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) < -5"), INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * -5 + 1, Long.MAX_VALUE)); // [26, 2^63 - 1]
        parseFilterAndCompare(ExpressionContext.of("($.apples / -5) <= -5"), INDEX_FILTER_INPUT,
                Filter.range("apples", -5 * -5, Long.MAX_VALUE)); // [25, 2^63 - 1]
    }

    @Test
    void div_binIsDivisor_leftNumberIsLarger() {
        parseFilterAndCompare(ExpressionContext.of("(90 / $.bananas) > 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 1, 90 / 10 - 1)); // [1,8]
        parseFilterAndCompare(ExpressionContext.of("(90 / $.bananas) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 1, 90 / 10)); // [1,9]

        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) < 10"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) <= 10"), INDEX_FILTER_INPUT)).isNull();

        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) > -10"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) >= -10"), INDEX_FILTER_INPUT)).isNull();
        parseFilterAndCompare(ExpressionContext.of("(90 / $.bananas) < -10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 90 / -10 + 1, -1)); // [-8, -1]
        parseFilterAndCompare(ExpressionContext.of("(90 / $.bananas) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 90 / -10, -1)); // [-8, -1]

        parseFilterAndCompare(ExpressionContext.of("(-90 / $.bananas) > 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", -90 / 10 + 1, -1)); // [-8, -1]
        parseFilterAndCompare(ExpressionContext.of("(90 / $.bananas) >= 10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 1, 90 / 10)); // [1,9]
        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) < 10"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) <= 10"), INDEX_FILTER_INPUT)).isNull();

        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) > -10"), INDEX_FILTER_INPUT)).isNull();
        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) >= -10"), INDEX_FILTER_INPUT)).isNull();
        parseFilterAndCompare(ExpressionContext.of("(-90 / $.bananas) < -10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 1L, -90 / -10 - 1)); // [1, 8]
        parseFilterAndCompare(ExpressionContext.of("(-90 / $.bananas) <= -10"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 1L, -90 / -10)); // [1, 9]
    }

    @Test
    void div_binIsDivisor_leftNumberIsSmaller() {
        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) >= 10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) < 10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) <= 10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) > -10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) >= -10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) < -10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) <= -10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers

        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) >= 10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) < 10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) <= 10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) > -10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) >= -10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) < -10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        assertThat(parseFilter(ExpressionContext.of("(-9 / $.bananas) <= -10"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers

        // Not supported by secondary index Filter
        assertThat(parseFilter(ExpressionContext.of("(0 / $.bananas) > 10"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        // Not supported by secondary index Filter, cannot divide by zero
        assertThat(parseFilter(ExpressionContext.of("(9 / $.bananas) > 0"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
    }

    @Test
    void div_binIsDivisor_leftNumberEqualsRight() {
        // Not supported by secondary index filter
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) > 90"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        parseFilterAndCompare(ExpressionContext.of("(90 / $.bananas) >= 90"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 90 / 90, 90 / 90)); // [1, 1]
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) < 90"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(90 / $.bananas) <= 90"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers

        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) > -90"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) >= -90"), INDEX_FILTER_INPUT)).isNull(); // maximal range is all numbers
        assertThat(parseFilter(ExpressionContext.of("(-90 / $.bananas) < -90"), INDEX_FILTER_INPUT)).isNull(); // no integer numbers
        parseFilterAndCompare(ExpressionContext.of("(-90 / $.bananas) <= -90"), INDEX_FILTER_INPUT,
                Filter.range("bananas", 1L, 90 / 90)); // [1, 1]
    }
}
