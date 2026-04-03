/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aerospike.dsl.client.fluent;

import com.aerospike.dsl.client.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Comparator that orders objects according to the Aerospike server's
 * type ordering hierarchy:
 * NIL(1) < BOOLEAN(2) < INTEGER(3) < STRING(4) < LIST(5) < MAP(6)
 * < BYTES(7) < DOUBLE(8) < GEOJSON(9).
 * <p>
 * Cross-type comparison is strictly by type ordinal — there is no
 * numeric promotion between INTEGER and DOUBLE.
 *
 * @see <a href="https://aerospike.com/docs/develop/data-types/collections/ordering/">
 *      Aerospike type ordering</a>
 */
public class AerospikeComparator implements Comparator<Object> {
    enum AsType {
        NULL (1),
        BOOLEAN (2),
        INTEGER (3),
        STRING (4),
        LIST (5),
        MAP (6),
        BYTES (7),
        DOUBLE (8),
        GEOJSON (9),
        OTHER (10);

        private final int value;
        AsType(int value) {
            this.value = value;
        }

        public int getOrdinal() {
            return value;
        }
    }

    private final boolean caseSensitiveStrings;

    public AerospikeComparator() {
        this(true);
    }

    public AerospikeComparator(boolean caseSensitiveStrings) {
        this.caseSensitiveStrings = caseSensitiveStrings;
    }

    private boolean isByteType(Class<?> clazz) {
        return Byte.class.equals(clazz) ||
            Byte.TYPE.equals(clazz);
    }

    private boolean isIntegerType(Object o) {
        return ((o instanceof Byte) || (o instanceof Character) || (o instanceof Short) || (o instanceof Integer) || (o instanceof Long));
    }
    private boolean isFloatType(Object o) {
        return ((o instanceof Float) || (o instanceof Double));
    }

    AsType getType(Object o) {
        if (o == null) { return AsType.NULL; }
        else if (o instanceof Boolean) { return AsType.BOOLEAN; }
        else if (isIntegerType(o)) { return AsType.INTEGER; }
        else if (o instanceof String) { return AsType.STRING; }
        else if (o instanceof List) { return AsType.LIST; }
        else if (o instanceof Map) { return AsType.MAP; }
        else if (o instanceof Value.HLLValue) { return AsType.BYTES; }
        else if (o.getClass().isArray() && isByteType(o.getClass().getComponentType())) { return AsType.BYTES; }
        else if (isFloatType(o)) { return AsType.DOUBLE; }
        else if (o instanceof Value.GeoJSONValue) { return AsType.GEOJSON; }
        else {
            return AsType.OTHER;
        }
    }

    private byte[] toByteArray(Object o) {
        if (o instanceof Value.HLLValue) {
            return ((Value.HLLValue) o).getBytes();
        }
        return (byte[]) o;
    }

    private int compareList(List<Object> l1, List<Object> l2) {
        int l1Size = l1.size();
        int l2Size = l2.size();
        for (int index = 0; index < l1Size; index++) {
            if (index >= l2Size) {
                return 1;
            }
            int result = compare(l1.get(index), l2.get(index));
            if (result != 0) {
                return result;
            }
        }
        return l1Size == l2Size ? 0 : -1;
    }

    private int compareMap(Map<Object, Object> m1, Map<Object, Object> m2) {
        if (m1.size() == m2.size()) {
            List<Object> sortedKeys1 = new ArrayList<>(m1.keySet());
            sortedKeys1.sort(this);
            List<Object> sortedKeys2 = new ArrayList<>(m2.keySet());
            sortedKeys2.sort(this);
            int result = compareList(sortedKeys1, sortedKeys2);
            if (result != 0) {
                return result;
            }
            for (int i = 0; i < sortedKeys1.size(); i++) {
                Object v1 = m1.get(sortedKeys1.get(i));
                Object v2 = m2.get(sortedKeys2.get(i));
                result = this.compare(v1, v2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
        else {
            return m1.size() - m2.size();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(Object o1, Object o2) {
        AsType t1 = getType(o1);
        AsType t2 = getType(o2);
        if (t1.getOrdinal() != t2.getOrdinal()) {
            return t1.getOrdinal() - t2.getOrdinal();
        }

        switch (t1) {
            case NULL:
                return 0;
            case BOOLEAN:
                return Boolean.compare((Boolean)o1, (Boolean)o2);
            case INTEGER:
                return Long.compare(((Number)o1).longValue(), ((Number)o2).longValue());
            case STRING:
                if (caseSensitiveStrings) {
                    return ((String)o1).compareTo((String)o2);
                }
                else {
                    return ((String)o1).compareToIgnoreCase((String)o2);
                }
            case LIST:
                return compareList((List<Object>)o1, (List<Object>)o2);
            case MAP:
                return compareMap((Map<Object, Object>)o1, (Map<Object, Object>)o2);
            case BYTES:
                return Arrays.compare(toByteArray(o1), toByteArray(o2));
            case DOUBLE:
                return Double.compare(((Number)o1).doubleValue(), ((Number)o2).doubleValue());
            case GEOJSON:
                return o1.toString().compareTo(o2.toString());
            case OTHER:
            default:
                throw new UnsupportedOperationException(
                    "Cannot compare objects of type: " + o1.getClass().getName());
        }
    }
}
