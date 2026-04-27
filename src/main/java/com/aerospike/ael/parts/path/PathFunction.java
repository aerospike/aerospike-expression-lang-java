package com.aerospike.ael.parts.path;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer.ExprPartsOperation;
import lombok.Getter;

@Getter
public class PathFunction extends AbstractPart {

    private final PathFunctionType pathFunctionType;
    private final ReturnParam returnParam;
    private final Exp.Type binType;

    public PathFunction(PathFunctionType pathFunctionType, ReturnParam returnParam, Exp.Type binType) {
        super(PartType.PATH_FUNCTION);
        this.pathFunctionType = pathFunctionType;
        this.returnParam = returnParam;
        this.binType = binType;
    }

    public static Exp.Type castTypeToExpType(CastType castType) {
        return switch (castType) {
            case INT -> Exp.Type.INT;
            case FLOAT -> Exp.Type.FLOAT;
        };
    }

    /**
     * Returns the source {@link Exp.Type} for a cast — the type the value must
     * already have before the cast is applied (e.g., casting to INT requires a FLOAT source).
     */
    public static Exp.Type castSourceExpType(CastType castType) {
        return switch (castType) {
            case INT -> Exp.Type.FLOAT;
            case FLOAT -> Exp.Type.INT;
        };
    }

    public static ExprPartsOperation castTypeToOperation(CastType castType) {
        return switch (castType) {
            case INT -> ExprPartsOperation.TO_INT;
            case FLOAT -> ExprPartsOperation.TO_FLOAT;
        };
    }

    public enum ReturnParam {
        VALUE,
        INDEX,
        RANK,
        COUNT,
        NONE,
        EXISTS,
        REVERSE_INDEX,
        REVERSE_RANK,
        KEY_VALUE,
        UNORDERED_MAP,
        ORDERED_MAP,
        KEY
    }

    public enum PathFunctionType {
        GET,
        COUNT,
        SIZE,
        CAST
    }

    public enum CastType {
        INT,
        FLOAT
    }
}
