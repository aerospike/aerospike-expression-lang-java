/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.ael.client.exp;

import com.aerospike.ael.client.AerospikeException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.util.Pack;

/**
 * Map expression generator. See {@link Exp}.
 * <p>
 * The bin expression argument in these methods can be a reference to a bin or the
 * result of another expression. Expressions that modify bin values are only used
 * for temporary expression evaluation and are not permanently applied to the bin.
 * <p>
 * Map modify expressions return the bin's value. This value will be a map except
 * when the map is nested within a list. In that case, a list is returned for the
 * map modify expression.
 * <p>
 * Valid map key types are:
 * <ul>
 * <li>String</li>
 * <li>Integer</li>
 * <li>byte[]</li>
 * </ul>
 * <p>
 * The server will validate map key types in an upcoming release.
 * <p>
 * All maps maintain an index and a rank.  The index is the item offset from the start of the map,
 * for both unordered and ordered maps.  The rank is the sorted index of the value component.
 * Map supports negative indexing for index and rank.
 * <p>
 * Index examples:
 * <ul>
 * <li>Index 0: First item in map.</li>
 * <li>Index 4: Fifth item in map.</li>
 * <li>Index -1: Last item in map.</li>
 * <li>Index -3: Third to last item in map.</li>
 * <li>Index 1 Count 2: Second and third items in map.</li>
 * <li>Index -3 Count 3: Last three items in map.</li>
 * <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
 * </ul>
 * <p>
 * Rank examples:
 * <ul>
 * <li>Rank 0: Item with lowest value rank in map.</li>
 * <li>Rank 4: Fifth lowest ranked item in map.</li>
 * <li>Rank -1: Item with highest ranked value in map.</li>
 * <li>Rank -3: Item with third highest ranked value in map.</li>
 * <li>Rank 1 Count 2: Second and third lowest ranked items in map.</li>
 * <li>Rank -3 Count 3: Top three ranked items in map.</li>
 * </ul>
 * <p>
 * Nested expressions are supported by optional CTX context arguments.  Example:
 * <ul>
 * <li>bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}</li>
 * <li>Set map value to 11 for map key "key21" inside of map key "key2".</li>
 * <li>Get size of map key2.</li>
 * <li>MapExp.size(Exp.mapBin("bin"), CTX.mapKey(Value.get("key2"))</li>
 * <li>result = 2</li>
 * </ul>
 */
public final class MapExp {
    private static final int MODULE = 0;
    private static final int PUT = 67;
    private static final int PUT_ITEMS = 68;
    private static final int REPLACE = 69;
    private static final int REPLACE_ITEMS = 70;
    private static final int INCREMENT = 73;
    private static final int CLEAR = 75;
    private static final int REMOVE_BY_KEY = 76;
    private static final int REMOVE_BY_INDEX = 77;
    private static final int REMOVE_BY_RANK = 79;
    private static final int REMOVE_BY_KEY_LIST = 81;
    private static final int REMOVE_BY_VALUE = 82;
    private static final int REMOVE_BY_VALUE_LIST = 83;
    private static final int REMOVE_BY_KEY_INTERVAL = 84;
    private static final int REMOVE_BY_INDEX_RANGE = 85;
    private static final int REMOVE_BY_VALUE_INTERVAL = 86;
    private static final int REMOVE_BY_RANK_RANGE = 87;
    private static final int REMOVE_BY_KEY_REL_INDEX_RANGE = 88;
    private static final int REMOVE_BY_VALUE_REL_RANK_RANGE = 89;
    private static final int SIZE = 96;
    private static final int GET_BY_KEY = 97;
    private static final int GET_BY_INDEX = 98;
    private static final int GET_BY_RANK = 100;
    private static final int GET_BY_VALUE = 102;  // GET_ALL_BY_VALUE on server.
    private static final int GET_BY_KEY_INTERVAL = 103;
    private static final int GET_BY_INDEX_RANGE = 104;
    private static final int GET_BY_VALUE_INTERVAL = 105;
    private static final int GET_BY_RANK_RANGE = 106;
    private static final int GET_BY_KEY_LIST = 107;
    private static final int GET_BY_VALUE_LIST = 108;
    private static final int GET_BY_KEY_REL_INDEX_RANGE = 109;
    private static final int GET_BY_VALUE_REL_RANK_RANGE = 110;

    /**
     * Create expression that returns list size.
     *
     * <pre>{@code
     * // Map bin "a" size > 7
     * Exp.gt(MapExp.size(Exp.mapBin("a")), Exp.val(7))
     * }</pre>
     */
    public static Exp size(Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(SIZE, ctx);
        return addRead(bin, bytes, Exp.Type.INT);
    }

    /**
     * Create expression that selects map item identified by key and returns selected data
     * specified by returnType.
     *
     * <pre>{@code
     * // Map bin "a" contains key "B"
     * MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val("B"), Exp.mapBin("a"))
     * }</pre>
     *
     * @param returnType	metadata attributes to return. See {@link MapReturnType}
     * @param valueType		expected type of return value
     * @param key			map key expression
     * @param bin			bin or map value expression
     * @param ctx			optional context path for nested CDT
     */
    public static Exp getByKey(int returnType, Exp.Type valueType, Exp key, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_KEY, returnType, key, ctx);
        return addRead(bin, bytes, valueType);
    }

    /**
     * Create expression that selects map items identified by key range (keyBegin inclusive, keyEnd exclusive).
     * If keyBegin is null, the range is less than keyEnd.
     * If keyEnd is null, the range is greater than equal to keyBegin.
     * <p>
     * Expression returns selected data specified by returnType (See {@link MapReturnType}).
     */
    public static Exp getByKeyRange(int returnType, Exp keyBegin, Exp keyEnd, Exp bin, CTX... ctx) {
        byte[] bytes = ListExp.packRangeOperation(GET_BY_KEY_INTERVAL, returnType, keyBegin, keyEnd, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items identified by keys and returns selected data specified by
     * returnType (See {@link MapReturnType}).
     */
    public static Exp getByKeyList(int returnType, Exp keys, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_KEY_LIST, returnType, keys, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items nearest to key and greater by index.
     * Expression returns selected data specified by returnType (See {@link MapReturnType}).
     * <p>
     * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
     * <ul>
     * <li>(value,index) = [selected items]</li>
     * <li>(5,0) = [{5=15},{9=10}]</li>
     * <li>(5,1) = [{9=10}]</li>
     * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
     * <li>(3,2) = [{9=10}]</li>
     * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
     * </ul>
     */
    public static Exp getByKeyRelativeIndexRange(int returnType, Exp key, Exp index, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_KEY_REL_INDEX_RANGE, returnType, key, index, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items nearest to key and greater by index with a count limit.
     * Expression returns selected data specified by returnType (See {@link MapReturnType}).
     * <p>
     * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
     * <ul>
     * <li>(value,index,count) = [selected items]</li>
     * <li>(5,0,1) = [{5=15}]</li>
     * <li>(5,1,2) = [{9=10}]</li>
     * <li>(5,-1,1) = [{4=2}]</li>
     * <li>(3,2,1) = [{9=10}]</li>
     * <li>(3,-2,2) = [{0=17}]</li>
     * </ul>
     */
    public static Exp getByKeyRelativeIndexRange(int returnType, Exp key, Exp index, Exp count, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_KEY_REL_INDEX_RANGE, returnType, key, index, count, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items identified by value and returns selected data
     * specified by returnType.
     *
     * <pre>{@code
     * // Map bin "a" contains value "BBB"
     * MapExp.getByValue(MapReturnType.EXISTS, Exp.val("BBB"), Exp.mapBin("a"))
     * }</pre>
     *
     * @param returnType	metadata attributes to return. See {@link MapReturnType}
     * @param value			value expression
     * @param bin			bin or map value expression
     * @param ctx			optional context path for nested CDT
     */
    public static Exp getByValue(int returnType, Exp value, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_VALUE, returnType, value, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
     * If valueBegin is null, the range is less than valueEnd.
     * If valueEnd is null, the range is greater than equal to valueBegin.
     * <p>
     * Expression returns selected data specified by returnType (See {@link MapReturnType}).
     */
    public static Exp getByValueRange(int returnType, Exp valueBegin, Exp valueEnd, Exp bin, CTX... ctx) {
        byte[] bytes = ListExp.packRangeOperation(GET_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items identified by values and returns selected data specified by
     * returnType (See {@link MapReturnType}).
     */
    public static Exp getByValueList(int returnType, Exp values, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_VALUE_LIST, returnType, values, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items nearest to value and greater by relative rank.
     * Expression returns selected data specified by returnType (See {@link MapReturnType}).
     * <p>
     * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
     * <ul>
     * <li>(value,rank) = [selected items]</li>
     * <li>(11,1) = [{0=17}]</li>
     * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
     * </ul>
     */
    public static Exp getByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map items nearest to value and greater by relative rank with a count limit.
     * Expression returns selected data specified by returnType (See {@link MapReturnType}).
     * <p>
     * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
     * <ul>
     * <li>(value,rank,count) = [selected items]</li>
     * <li>(11,1,1) = [{0=17}]</li>
     * <li>(11,-1,1) = [{9=10}]</li>
     * </ul>
     */
    public static Exp getByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp count, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map item identified by index and returns selected data specified by
     * returnType (See {@link MapReturnType}).
     */
    public static Exp getByIndex(int returnType, Exp.Type valueType, Exp index, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_INDEX, returnType, index, ctx);
        return addRead(bin, bytes, valueType);
    }

    /**
     * Create expression that selects map items starting at specified index to the end of map and returns selected
     * data specified by returnType (See {@link MapReturnType}).
     */
    public static Exp getByIndexRange(int returnType, Exp index, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_INDEX_RANGE, returnType, index, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects "count" map items starting at specified index and returns selected data
     * specified by returnType (See {@link MapReturnType}).
     */
    public static Exp getByIndexRange(int returnType, Exp index, Exp count, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_INDEX_RANGE, returnType, index, count, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects map item identified by rank and returns selected data specified by
     * returnType (See {@link MapReturnType}).
     */
    public static Exp getByRank(int returnType, Exp.Type valueType, Exp rank, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_RANK, returnType, rank, ctx);
        return addRead(bin, bytes, valueType);
    }

    /**
     * Create expression that selects map items starting at specified rank to the last ranked item and
     * returns selected data specified by returnType (See {@link MapReturnType}).
     */
    public static Exp getByRankRange(int returnType, Exp rank, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_RANK_RANGE, returnType, rank, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    /**
     * Create expression that selects "count" map items starting at specified rank and returns selected
     * data specified by returnType (See {@link MapReturnType}).
     */
    public static Exp getByRankRange(int returnType, Exp rank, Exp count, Exp bin, CTX... ctx) {
        byte[] bytes = Pack.pack(GET_BY_RANK_RANGE, returnType, rank, count, ctx);
        return addRead(bin, bytes, getValueType(returnType));
    }

    private static Exp addWrite(Exp bin, byte[] bytes, CTX[] ctx) {
        int retType;

        if (ctx == null || ctx.length == 0) {
            retType = Exp.Type.MAP.code;
        }
        else {
            retType = ((ctx[0].id & 0x10) == 0)? Exp.Type.MAP.code : Exp.Type.LIST.code;
        }
        return new Exp.Module(bin, bytes, retType, MODULE | Exp.MODIFY);
    }

    private static Exp addRead(Exp bin, byte[] bytes, Exp.Type retType) {
        return new Exp.Module(bin, bytes, retType.code, MODULE);
    }

    private static Exp.Type getValueType(int returnType) {
        int t = returnType & ~MapReturnType.INVERTED;

        switch (t) {
            case MapReturnType.INDEX:
            case MapReturnType.REVERSE_INDEX:
            case MapReturnType.RANK:
            case MapReturnType.REVERSE_RANK:
                // This method only called from expressions that can return multiple integers (ie list).
                return Exp.Type.LIST;

            case MapReturnType.COUNT:
                return Exp.Type.INT;

            case MapReturnType.KEY:
            case MapReturnType.VALUE:
                // This method only called from expressions that can return multiple objects (ie list).
                return Exp.Type.LIST;

            case MapReturnType.KEY_VALUE:
            case MapReturnType.ORDERED_MAP:
            case MapReturnType.UNORDERED_MAP:
                return Exp.Type.MAP;

            case MapReturnType.EXISTS:
                return Exp.Type.BOOL;

            default:
            case MapReturnType.NONE:
                throw new AerospikeException("Invalid MapReturnType: " + returnType);
        }
    }
}
