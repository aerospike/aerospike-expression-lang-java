package com.aerospike.ael.util;

import com.aerospike.ael.AelParseException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import com.aerospike.ael.parts.cdt.list.ListValueRange;
import com.aerospike.ael.parts.cdt.map.MapKeyRange;
import com.aerospike.ael.parts.cdt.map.MapValueRange;

import static com.aerospike.ael.util.ParsingUtils.halfOpenRangeCount;
import static com.aerospike.ael.util.ParsingUtils.objectToExp;
import static com.aerospike.ael.util.ParsingUtils.requireSupportedExpValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParsingUtilsTests {

    @Nested
    class RequireSupportedExpValue {

        @Test
        void acceptsString() {
            assertThatNoException()
                    .isThrownBy(() -> requireSupportedExpValue("hello", "test"));
        }

        @Test
        void acceptsInteger() {
            assertThatNoException()
                    .isThrownBy(() -> requireSupportedExpValue(42, "test"));
        }

        @Test
        void acceptsLong() {
            assertThatNoException()
                    .isThrownBy(() -> requireSupportedExpValue(42L, "test"));
        }

        @Test
        void acceptsByteArray() {
            assertThatNoException()
                    .isThrownBy(() -> requireSupportedExpValue(new byte[]{1, 2}, "test"));
        }

        @Test
        @SuppressWarnings("DataFlowIssue")
        void rejectsNull() {
            assertThatThrownBy(() -> requireSupportedExpValue(null, "ctx"))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Null value in ctx");
        }

        @Test
        void rejectsDouble() {
            assertThatThrownBy(() -> requireSupportedExpValue(3.14, "ctx"))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Unsupported value type in ctx")
                    .hasMessageContaining("Double");
        }

        @Test
        void rejectsBoolean() {
            assertThatThrownBy(() -> requireSupportedExpValue(true, "ctx"))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Unsupported value type in ctx")
                    .hasMessageContaining("Boolean");
        }

        @Test
        void rejectsList() {
            assertThatThrownBy(() -> requireSupportedExpValue(List.of(1), "ctx"))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Unsupported value type in ctx");
        }
    }

    @Nested
    class ObjectToExp {

        @Test
        @SuppressWarnings("DataFlowIssue")
        void rejectsNull() {
            assertThatThrownBy(() -> objectToExp(null))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Cannot convert null to Exp");
        }

        @Test
        void rejectsUnsupportedType() {
            assertThatThrownBy(() -> objectToExp(3.14))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("Unsupported value type for Exp conversion")
                    .hasMessageContaining("Double");
        }
    }

    @Nested
    class HalfOpenRangeCount {

        @Test
        void endNullYieldsNullForTailSemantics() {
            assertThat(halfOpenRangeCount(1, null)).isNull();
        }

        @Test
        void startNullYieldsEndForOpenLower() {
            assertThat(halfOpenRangeCount(null, 3)).isEqualTo(3);
        }

        @Test
        void bothSetYieldsDifference() {
            assertThat(halfOpenRangeCount(1, 3)).isEqualTo(2);
        }

        @Test
        void invertedSpanIsNegative() {
            assertThat(halfOpenRangeCount(5, 2)).isEqualTo(-3);
        }
    }

    @Nested
    class OpenRangePartConstructors {

        @Test
        void mapKeyRange_rejectsBothBoundsNull() {
            assertThatThrownBy(() -> new MapKeyRange(false, null, null))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("MapKeyRange requires at least one bound");
        }

        @Test
        void mapValueRange_rejectsBothBoundsNull() {
            assertThatThrownBy(() -> new MapValueRange(false, null, null))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("MapValueRange requires at least one bound");
        }

        @Test
        void listValueRange_rejectsBothBoundsNull() {
            assertThatThrownBy(() -> new ListValueRange(false, null, null))
                    .isInstanceOf(AelParseException.class)
                    .hasMessageContaining("ListValueRange requires at least one bound");
        }
    }
}
