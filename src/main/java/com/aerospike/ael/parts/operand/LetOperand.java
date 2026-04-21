package com.aerospike.ael.parts.operand;

import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

@Getter
public class LetOperand extends AbstractPart {

    private final String string;
    @Setter
    private AbstractPart part;
    private final boolean isLastPart;

    public LetOperand(AbstractPart part, String string) {
        super(PartType.LET_OPERAND);
        this.string = string;
        this.part = part;
        this.isLastPart = false;
    }

    public LetOperand(AbstractPart part, boolean isLastPart) {
        super(PartType.LET_OPERAND);
        this.string = null;
        this.part = part;
        this.isLastPart = isLastPart;
    }
}
