package com.aerospike.ael.parts.controlstructure;

import com.aerospike.ael.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class WhenStructure extends AbstractPart {

    private List<AbstractPart> operands;

    public WhenStructure(List<AbstractPart> operands) {
        super(PartType.WHEN_STRUCTURE);
        this.operands = operands;
    }
}
