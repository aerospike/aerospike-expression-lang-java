package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;

@Getter
public class OrStructure extends AbstractPart {

    private final List<ExpressionContainer> operands;

    public OrStructure(List<ExpressionContainer> operands) {
        super(PartType.OR_STRUCTURE);
        this.operands = operands;
    }
}
