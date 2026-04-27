package com.aerospike.ael.util;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.exp.Exp;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ValidationUtils {

    /**
     * Validates if two {@link Exp.Type} instances are comparable.
     * Comparison is allowed if both types are the same, or if one is {@code INT} and the other is {@code FLOAT}.
     * If the types are not comparable, a {@link AelParseException} is thrown.
     *
     * @param leftType The {@link Exp.Type} of the left operand. Can be {@code null}
     * @param rightType The {@link Exp.Type} of the right operand. Can be {@code null}
     * @throws AelParseException If both types are not {@code null} and are not comparable
     */
    public static void validateComparableTypes(Exp.Type leftType, Exp.Type rightType) {
        if (leftType != null && rightType != null) {
            boolean isIntAndFloat =
                    (leftType.equals(Exp.Type.INT) && rightType.equals(Exp.Type.FLOAT)) ||
                            (leftType.equals(Exp.Type.FLOAT) && rightType.equals(Exp.Type.INT));

            if (!leftType.equals(rightType) && !isIntAndFloat) {
                throw new AelParseException("Cannot compare %s to %s".formatted(leftType, rightType));
            }
        }
    }
}
