package com.aerospike.ael.parts.operand;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

@Getter
public class VariableOperand extends AbstractPart implements ParsedValueOperand {

    private final String value;

    public VariableOperand(String name) {
        super(PartType.VARIABLE_OPERAND);
        this.value = name;
    }

    @Override
    public Exp getExp() {
        return Exp.var(value);
    }
}
