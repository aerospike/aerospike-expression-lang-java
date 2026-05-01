package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class ExclusiveStructure extends AbstractPart {

    private final List<AbstractPart> operands;

    public ExclusiveStructure(List<AbstractPart> operands) {
        super(PartType.EXCLUSIVE_STRUCTURE);
        this.operands = operands;
    }
}
