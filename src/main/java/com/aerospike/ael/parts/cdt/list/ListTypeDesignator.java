package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.path.BasePath;

import java.util.Collections;
import java.util.List;

/**
 * Designates that element to the left is a List.
 */
public class ListTypeDesignator extends ListPart {

    public ListTypeDesignator() {
        super(ListPartType.LIST_TYPE_DESIGNATOR);
    }

    public static ListTypeDesignator from() {
        return new ListTypeDesignator();
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        List<AbstractPart> partsUpToDesignator = !basePath.getCdtParts().isEmpty()
                ? basePath.getCdtParts().subList(0, basePath.getCdtParts().size() - 1)
                : Collections.emptyList();

        BasePath basePathUntilDesignator = new BasePath(basePath.getBinPart(), partsUpToDesignator);
        if (!partsUpToDesignator.isEmpty()) {
            return ((CdtPart) partsUpToDesignator.get(partsUpToDesignator.size() - 1))
                    .constructExp(basePathUntilDesignator, valueType, cdtReturnType, context);
        }
        // only bin
        return Exp.listBin(basePath.getBinPart().getBinName());
    }
}
