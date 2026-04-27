package com.aerospike.ael.parts.operand;

import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

import java.util.List;

@Getter
public class FunctionArgs extends AbstractPart {

    private final List<AbstractPart> operands;

    public FunctionArgs(List<AbstractPart> operands) {
        super(PartType.FUNCTION_ARGS);
        this.operands = List.copyOf(operands);
    }
}
