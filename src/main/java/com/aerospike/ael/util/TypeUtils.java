package com.aerospike.ael.util;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.cdt.map.MapTypeDesignator;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TypeUtils {

    /**
     * Returns the default {@link Exp.Type} for a given {@link AbstractPart}.
     * For {@link AbstractPart.PartType#MAP_PART} that is not a {@link MapTypeDesignator}, the default type is {@code STRING}.
     * Otherwise, the default type is {@code INT}.
     *
     * @param part The {@link AbstractPart} for which to determine the default type
     * @return The default {@link Exp.Type} for the given part
     */
    public static Exp.Type getDefaultType(AbstractPart part) {
        if (part.getPartType() == AbstractPart.PartType.MAP_PART
                // MapTypeDesignator is usually combined with int based operations such as size
                && !(part instanceof MapTypeDesignator)) {
            // For all other Map parts the default type should be STRING
            return Exp.Type.STRING;
        } else {
            // Default INT
            return Exp.Type.INT;
        }
    }

    // When return type is COUNT, always return INT as default type
    public static Exp.Type getDefaultTypeForCount() {
        return Exp.Type.INT;
    }
}
