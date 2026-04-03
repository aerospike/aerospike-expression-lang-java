package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

@Getter
public class BlobOperand extends AbstractPart implements ParsedValueOperand {

    private final byte[] value;

    public BlobOperand(byte[] value) {
        super(PartType.BLOB_OPERAND);
        this.value = value;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
