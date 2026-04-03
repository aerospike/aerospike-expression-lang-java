package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.DslParseException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OperandFactoryTests {

    @Test
    void negMapWithIncomparableKeys() {
        Map<Object, Object> unsupportedKeyMap = new HashMap<>();
        unsupportedKeyMap.put(new Object(), "a");
        unsupportedKeyMap.put(new Object(), "b");
        assertThatThrownBy(() -> OperandFactory.createOperand(unsupportedKeyMap))
                .isInstanceOf(DslParseException.class)
                .hasMessageContaining("mutually comparable");
    }

    @Test
    void mapWithMixedComparableKeys() {
        Map<Object, Object> mixedKeyMap = new HashMap<>();
        mixedKeyMap.put(1, "a");
        mixedKeyMap.put("b", 2);
        assertThat(OperandFactory.createOperand(mixedKeyMap)).isInstanceOf(MapOperand.class);
    }

    @Test
    void mapWithNullKey() {
        Map<Object, Object> nullKeyMap = new HashMap<>();
        nullKeyMap.put(null, "value");
        assertThat(OperandFactory.createOperand(nullKeyMap)).isInstanceOf(MapOperand.class);
    }
}
