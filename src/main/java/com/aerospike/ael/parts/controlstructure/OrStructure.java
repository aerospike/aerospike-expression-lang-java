package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class OrStructure extends AbstractPart {

    private final List<AbstractPart> operands;

    public OrStructure(List<AbstractPart> operands) {
        super(PartType.OR_STRUCTURE);
        this.operands = operands;
    }
}
