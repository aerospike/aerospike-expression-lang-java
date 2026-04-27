package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.path.PathFunction;
import lombok.Getter;

@Getter
public abstract class MapPart extends CdtPart {

    private final MapPartType mapPartType;

    protected MapPart(MapPartType mapPartType) {
        super(PartType.MAP_PART);
        this.mapPartType = mapPartType;
    }

    @Override
    public int getReturnType(PathFunction.ReturnParam returnParam) {
        return switch (returnParam) {
            case VALUE -> MapReturnType.VALUE;
            case KEY_VALUE -> MapReturnType.KEY_VALUE;
            case UNORDERED_MAP -> MapReturnType.UNORDERED_MAP;
            case ORDERED_MAP -> MapReturnType.ORDERED_MAP;
            case KEY -> MapReturnType.KEY;
            case INDEX -> MapReturnType.INDEX;
            case RANK -> MapReturnType.RANK;
            case COUNT, NONE -> MapReturnType.COUNT;
            case EXISTS -> MapReturnType.EXISTS;
            case REVERSE_INDEX -> MapReturnType.REVERSE_INDEX;
            case REVERSE_RANK -> MapReturnType.REVERSE_RANK;
        };
    }

    public enum MapPartType {
        MAP_TYPE_DESIGNATOR,
        KEY,
        INDEX,
        VALUE,
        RANK,
        KEY_RANGE,
        KEY_LIST,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE,
        RANK_RANGE,
        RANK_RANGE_RELATIVE,
        INDEX_RANGE_RELATIVE
    }
}
