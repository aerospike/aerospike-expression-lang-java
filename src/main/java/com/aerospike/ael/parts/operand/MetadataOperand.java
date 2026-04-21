package com.aerospike.ael.parts.operand;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.parts.ExpressionContainer;
import lombok.Getter;

@Getter
public class MetadataOperand extends ExpressionContainer {

    private final String functionName;
    private final Integer parameter;

    public MetadataOperand(String functionName) {
        super();
        this.partType = PartType.METADATA_OPERAND;
        this.functionName = functionName;
        this.parameter = null;
    }

    public MetadataOperand(String functionName, int parameter) {
        super();
        this.partType = PartType.METADATA_OPERAND;
        this.functionName = functionName;
        this.parameter = parameter;
    }

    private Exp constructMetadataExp(String functionName, Integer parameter) {
        return switch (functionName) {
            case "deviceSize", "recordSize", "memorySize" -> Exp.recordSize();
            case "digestModulo" -> Exp.digestModulo(parameter);
            case "isTombstone" -> Exp.isTombstone();
            case "keyExists" -> Exp.keyExists();
            case "lastUpdate" -> Exp.lastUpdate();
            case "sinceUpdate" -> Exp.sinceUpdate();
            case "setName" -> Exp.setName();
            case "ttl" -> Exp.ttl();
            case "voidTime" -> Exp.voidTime();
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };
    }

    @Override
    public Exp getExp() {
        return constructMetadataExp(functionName, parameter);
    }

    public MetadataReturnType getMetadataType() {
        return switch (functionName) {
            case "deviceSize",
                 "memorySize",
                 "recordSize",
                 "digestModulo",
                 "lastUpdate",
                 "sinceUpdate",
                 "ttl",
                 "voidTime" -> MetadataReturnType.INT;
            case "isTombstone",
                 "keyExists" -> MetadataReturnType.BOOL;
            case "setName" -> MetadataReturnType.STRING;
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };
    }

    public enum MetadataReturnType {
        INT,
        STRING,
        BOOL,
    }
}
