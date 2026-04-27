package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.path.BasePath;

/**
 * Designates that the element to the left is a Map.
 */
public class MapTypeDesignator extends MapPart {

    public MapTypeDesignator() {
        super(MapPartType.MAP_TYPE_DESIGNATOR);
    }

    public static MapTypeDesignator from() {
        return new MapTypeDesignator();
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        var partsUpToDesignator = basePath.getCdtParts().subList(0, basePath.getCdtParts().size() - 1);
        BasePath basePathUntilDesignator = new BasePath(basePath.getBinPart(), partsUpToDesignator);
        int partsUpToDesignatorSize = partsUpToDesignator.size();

        if ((partsUpToDesignatorSize > 0)) {
            return ((CdtPart) partsUpToDesignator.get(partsUpToDesignatorSize - 1))
                    .constructExp(basePathUntilDesignator, valueType, cdtReturnType, context);
        }
        // only bin
        return Exp.mapBin(basePath.getBinPart().getBinName());
    }
}
