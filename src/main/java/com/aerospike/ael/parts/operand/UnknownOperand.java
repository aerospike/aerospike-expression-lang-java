package com.aerospike.ael.parts.operand;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;

/**
 * AST node for the {@code unknown} and {@code error} keywords.
 * Both compile to {@link Exp#unknown()}, which throws a server-side exception on evaluation.
 */
public class UnknownOperand extends AbstractPart {

    public UnknownOperand() {
        super(PartType.UNKNOWN_OPERAND);
    }

    @Override
    public Exp getExp() {
        return Exp.unknown();
    }
}
