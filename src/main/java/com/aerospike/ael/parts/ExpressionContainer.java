package com.aerospike.ael.parts;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.EnumSet;

@Getter
public class ExpressionContainer extends AbstractPart {

    @Setter
    protected AbstractPart left;
    @Setter
    protected AbstractPart right;
    private final boolean isUnary;
    private final ExprPartsOperation operationType;
    @Setter()
    @Accessors(fluent = true)
    private boolean hasSecondaryIndexFilter;
    @Setter()
    @Accessors(fluent = true)
    private boolean isExclFromSecondaryIndexFilter;

    public ExpressionContainer() {
        super(PartType.EXPRESSION_CONTAINER);
        this.isUnary = false;
        this.left = null;
        this.right = null;
        this.operationType = null;
    }

    public ExpressionContainer(AbstractPart left, AbstractPart right, ExprPartsOperation operationType) {
        super(PartType.EXPRESSION_CONTAINER);
        this.left = left;
        this.right = right;
        this.operationType = operationType;
        this.isUnary = false;
    }

    public ExpressionContainer(AbstractPart singleOperand, ExprPartsOperation operationType) {
        super(PartType.EXPRESSION_CONTAINER);
        this.left = singleOperand;
        this.right = null;
        this.operationType = operationType;
        this.isUnary = true;
    }

    public enum ExprPartsOperation {
        ADD,
        SUB,
        DIV,
        MUL,
        MOD,
        POW,
        INT_XOR,
        INT_NOT, // unary
        INT_AND,
        INT_OR,
        L_SHIFT,
        R_SHIFT,
        LOGICAL_R_SHIFT,
        ABS, // unary
        CEIL, // unary
        FLOOR, // unary
        LOG,
        MIN_FUNC,
        MAX_FUNC,
        COUNT_ONE_BITS, // unary
        FIND_BIT_LEFT,
        FIND_BIT_RIGHT,
        TO_INT, // unary
        TO_FLOAT, // unary
        AND,
        OR,
        NOT, // unary
        EQ,
        NOTEQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        IN,
        LET_STRUCTURE, // unary
        WHEN_STRUCTURE, // unary
        EXCLUSIVE_STRUCTURE, // unary
        AND_STRUCTURE,
        OR_STRUCTURE,
        EXISTS; // unary

        // New values not in this set default to "might produce a list" (no false positives).
        private static final EnumSet<ExprPartsOperation> SCALAR = EnumSet.of(
                ADD, SUB, MUL, DIV, MOD, POW,
                INT_AND, INT_OR, INT_XOR, INT_NOT,
                L_SHIFT, R_SHIFT, LOGICAL_R_SHIFT,
                ABS, CEIL, FLOOR, LOG,
                MIN_FUNC, MAX_FUNC,
                COUNT_ONE_BITS, FIND_BIT_LEFT, FIND_BIT_RIGHT,
                TO_INT, TO_FLOAT,
                EQ, NOTEQ, GT, GTEQ, LT, LTEQ,
                IN, NOT, AND, OR,
                AND_STRUCTURE, OR_STRUCTURE, EXCLUSIVE_STRUCTURE,
                EXISTS
        );

        public boolean isScalar() {
            return SCALAR.contains(this);
        }
    }
}
