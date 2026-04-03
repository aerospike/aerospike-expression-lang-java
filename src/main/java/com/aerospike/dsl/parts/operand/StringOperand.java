package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

import java.util.Base64;

@Getter
public class StringOperand extends AbstractPart implements ParsedValueOperand {

    private final String value;
    @Setter
    private boolean isBlob = false;

    public StringOperand(String string) {
        super(PartType.STRING_OPERAND);
        this.value = string;
    }

    @Override
    public Exp getExp() {
        if (isBlob) {
            try {
                byte[] byteValue = Base64.getDecoder().decode(value);
                return Exp.val(byteValue);
            } catch (IllegalArgumentException e) {
                throw new DslParseException(
                        "String compared to BLOB-typed path is not valid Base64: " + value, e);
            }
        }
        return Exp.val(value);
    }
}
