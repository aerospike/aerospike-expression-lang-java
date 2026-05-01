package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class AndStructure extends AbstractPart {

    private final List<AbstractPart> operands;

    public AndStructure(List<AbstractPart> operands) {
        super(PartType.AND_STRUCTURE);
        this.operands = operands;
    }
}
