package com.aerospike.ael.parts.operand;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

import java.util.SortedMap;

@Getter
public class MapOperand extends AbstractPart implements ParsedValueOperand {

    private final SortedMap<Object, Object> value;

    public MapOperand(SortedMap<Object, Object> map) {
        super(PartType.MAP_OPERAND);
        this.value = map;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
