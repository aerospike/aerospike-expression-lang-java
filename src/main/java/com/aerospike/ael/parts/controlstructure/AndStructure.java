package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;

@Getter
public class AndStructure extends AbstractPart {

    private final List<ExpressionContainer> operands;

    public AndStructure(List<ExpressionContainer> operands) {
        super(PartType.AND_STRUCTURE);
        this.operands = operands;
    }
}
