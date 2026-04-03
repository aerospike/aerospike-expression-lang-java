package com.aerospike.dsl.parts;

import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.query.Filter;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractPart {

    protected Exp.Type expType;
    protected PartType partType;
    protected Exp exp;
    protected Filter filter;
    protected CTX[] ctx;
    protected boolean isPlaceholder;

    protected AbstractPart(PartType partType) {
        this.partType = partType;
        this.exp = null;
    }

    public enum PartType {
        INT_OPERAND,
        FLOAT_OPERAND,
        BOOL_OPERAND,
        STRING_OPERAND,
        LIST_OPERAND,
        MAP_OPERAND,
        LET_OPERAND,
        LET_STRUCTURE,
        WHEN_STRUCTURE,
        EXCLUSIVE_STRUCTURE,
        AND_STRUCTURE,
        OR_STRUCTURE,
        BASE_PATH,
        BIN_PART,
        LIST_PART,
        MAP_PART,
        PATH_OPERAND,
        PATH_FUNCTION,
        METADATA_OPERAND,
        EXPRESSION_CONTAINER,
        VARIABLE_OPERAND,
        PLACEHOLDER_OPERAND,
        FUNCTION_ARGS,
        BLOB_OPERAND
    }
}
