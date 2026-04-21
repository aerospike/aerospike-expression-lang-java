package com.aerospike.ael.parts.cdt;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.parts.path.PathFunction;

public abstract class CdtPart extends AbstractPart {

    protected CdtPart(PartType partType) {
        super(partType);
    }

    public abstract Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context);

    public CTX getContext() {
        // should print the subclass of the cdt type
        throw new AelParseException("Context is not supported for %s".formatted(this.getClass().getName()));
    }

    public abstract int getReturnType(PathFunction.ReturnParam returnParam);

    public static boolean isCdtPart(AbstractPart part) {
        return part.getPartType() == AbstractPart.PartType.LIST_PART
                || part.getPartType() == AbstractPart.PartType.MAP_PART;
    }
}
