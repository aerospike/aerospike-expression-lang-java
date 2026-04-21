package com.aerospike.ael.parts.operand;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;

import static com.aerospike.ael.parts.AbstractPart.PartType.BOOL_OPERAND;

@Getter
public class BooleanOperand extends AbstractPart implements ParsedValueOperand {

    // Keeping the boxed type for interface compatibility
    private final Boolean value;

    public BooleanOperand(Boolean value) {
        // Setting parent type
        super(BOOL_OPERAND);
        this.value = value;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
