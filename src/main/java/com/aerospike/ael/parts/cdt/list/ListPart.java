package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.path.PathFunction;
import lombok.Getter;

@Getter
public abstract class ListPart extends CdtPart {

    private final ListPartType listPartType;

    protected ListPart(ListPartType listPartType) {
        super(PartType.LIST_PART);
        this.listPartType = listPartType;
    }

    @Override
    public int getReturnType(PathFunction.ReturnParam returnParam) {
        return switch (returnParam) {
            case VALUE -> ListReturnType.VALUE;
            case INDEX -> ListReturnType.INDEX;
            case RANK -> ListReturnType.RANK;
            case COUNT, NONE -> ListReturnType.COUNT;
            case EXISTS -> ListReturnType.EXISTS;
            case REVERSE_INDEX -> ListReturnType.REVERSE_INDEX;
            case REVERSE_RANK -> ListReturnType.REVERSE_RANK;
            default ->
                    throw new AelParseException("Unsupported Return Param for List CDT: %s".formatted(returnParam));
        };
    }

    public enum ListPartType {
        LIST_TYPE_DESIGNATOR,
        INDEX,
        VALUE,
        RANK,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE,
        RANK_RANGE,
        RANK_RANGE_RELATIVE
    }
}
