package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;

@Getter
public class ExclusiveStructure extends AbstractPart {

    private final List<ExpressionContainer> operands;

    public ExclusiveStructure(List<ExpressionContainer> operands) {
        super(PartType.EXCLUSIVE_STRUCTURE);
        this.operands = operands;
    }
}
