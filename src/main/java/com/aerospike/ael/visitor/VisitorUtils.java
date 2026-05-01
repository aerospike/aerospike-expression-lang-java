package com.aerospike.ael.visitor;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.Index;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.client.query.IndexType;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer;
import com.aerospike.ael.parts.ExpressionContainer.ExprPartsOperation;
import com.aerospike.ael.parts.controlstructure.AndStructure;
import com.aerospike.ael.parts.controlstructure.ExclusiveStructure;
import com.aerospike.ael.parts.controlstructure.LetStructure;
import com.aerospike.ael.parts.controlstructure.OrStructure;
import com.aerospike.ael.parts.controlstructure.WhenStructure;
import com.aerospike.ael.parts.operand.BlobOperand;
import com.aerospike.ael.parts.operand.FunctionArgs;
import com.aerospike.ael.parts.operand.IntOperand;
import com.aerospike.ael.parts.operand.LetOperand;
import com.aerospike.ael.parts.operand.ListOperand;
import com.aerospike.ael.parts.operand.MetadataOperand;
import com.aerospike.ael.parts.operand.PlaceholderOperand;
import com.aerospike.ael.parts.operand.StringOperand;
import com.aerospike.ael.parts.path.BinPart;
import com.aerospike.ael.parts.path.Path;
import com.aerospike.ael.util.TypeUtils;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.aerospike.ael.parts.AbstractPart.PartType.*;
import static com.aerospike.ael.parts.ExpressionContainer.ExprPartsOperation.*;
import static com.aerospike.ael.util.ValidationUtils.validateComparableTypes;
import static com.aerospike.ael.visitor.VisitorUtils.ArithmeticTermType.*;

@UtilityClass
public class VisitorUtils {

    protected enum FilterOperationType {
        GT,
        GTEQ,
        LT,
        LTEQ,
        EQ,
        NOTEQ
    }

    protected enum ArithmeticTermType {
        ADDEND,
        SUBTR,
        MIN,
        DIVIDEND,
        DIVISOR,
        MULTIPLICAND,
        MULTIPLIER,
    }

    private final Map<Exp.Type, IndexType> expTypeToIndexType = Map.of(
            Exp.Type.INT, IndexType.NUMERIC,
            Exp.Type.STRING, IndexType.STRING,
            Exp.Type.BLOB, IndexType.BLOB
    );

    final Map<AbstractPart.PartType, Exp.Type> partTypeToExpType = Map.of(
            AbstractPart.PartType.INT_OPERAND, Exp.Type.INT,
            AbstractPart.PartType.FLOAT_OPERAND, Exp.Type.FLOAT,
            AbstractPart.PartType.STRING_OPERAND, Exp.Type.STRING,
            AbstractPart.PartType.BOOL_OPERAND, Exp.Type.BOOL,
            AbstractPart.PartType.BLOB_OPERAND, Exp.Type.BLOB
    );

    /**
     * Converts an {@link ExprPartsOperation} enum value to its corresponding {@link FilterOperationType}.
     *
     * @param exprPartsOperation The {@link ExprPartsOperation} to convert
     * @return The corresponding {@link FilterOperationType}
     * @throws NoApplicableFilterException if the {@link ExprPartsOperation} has no matching {@link FilterOperationType}
     */
    private static FilterOperationType getFilterOperation(ExprPartsOperation exprPartsOperation) {
        return switch (exprPartsOperation) {
            case GT -> FilterOperationType.GT;
            case GTEQ -> FilterOperationType.GTEQ;
            case LT -> FilterOperationType.LT;
            case LTEQ -> FilterOperationType.LTEQ;
            case EQ -> FilterOperationType.EQ;
            case NOTEQ -> FilterOperationType.NOTEQ;
            default -> throw new NoApplicableFilterException("ExprPartsOperation has no matching FilterOperationType");
        };
    }

    /**
     * Gets the appropriate {@link BinaryOperator<Exp>} for a given {@link ExprPartsOperation}.
     *
     * @param exprPartsOperation The {@link ExprPartsOperation} for which to get the binary operator
     * @return A {@link BinaryOperator<Exp>} that performs the corresponding operation on two {@link Exp} operands
     * @throws NoApplicableFilterException if the {@link ExprPartsOperation} has no matching {@link BinaryOperator<Exp>}
     */
    private static BinaryOperator<Exp> getExpOperator(ExprPartsOperation exprPartsOperation) {
        return switch (exprPartsOperation) {
            case ADD -> Exp::add;
            case SUB -> Exp::sub;
            case MUL -> Exp::mul;
            case DIV -> Exp::div;
            case MOD -> Exp::mod;
            case POW -> Exp::pow;
            case LOG -> Exp::log;
            case FIND_BIT_LEFT -> Exp::lscan;
            case FIND_BIT_RIGHT -> Exp::rscan;
            case INT_XOR -> Exp::intXor;
            case L_SHIFT -> Exp::lshift;
            case R_SHIFT -> Exp::arshift;
            case LOGICAL_R_SHIFT -> Exp::rshift;
            case INT_AND -> Exp::intAnd;
            case INT_OR -> Exp::intOr;
            case AND -> Exp::and;
            case OR -> Exp::or;
            case EQ -> Exp::eq;
            case NOTEQ -> Exp::ne;
            case LT -> Exp::lt;
            case LTEQ -> Exp::le;
            case GT -> Exp::gt;
            case GTEQ -> Exp::ge;
            default -> throw new NoApplicableFilterException("ExprPartsOperation has no matching BinaryOperator<Exp>");
        };
    }

    /**
     * Gets the appropriate {@link UnaryOperator<Exp>} for a given {@link ExprPartsOperation}.
     *
     * @param exprPartsOperation The {@link ExprPartsOperation} for which to get the unary operator
     * @return A {@link UnaryOperator<Exp>} that performs the corresponding operation on a single {@link Exp} operand
     * @throws NoApplicableFilterException if the {@link ExprPartsOperation} has no matching {@link UnaryOperator<Exp>}
     */
    private static UnaryOperator<Exp> getUnaryExpOperator(ExprPartsOperation exprPartsOperation) {
        return switch (exprPartsOperation) {
            case INT_NOT -> Exp::intNot;
            case NOT -> Exp::not;
            case ABS -> Exp::abs;
            case CEIL -> Exp::ceil;
            case FLOOR -> Exp::floor;
            case COUNT_ONE_BITS -> Exp::count;
            case TO_INT -> Exp::toInt;
            case TO_FLOAT -> Exp::toFloat;
            default -> throw new NoApplicableFilterException("ExprPartsOperation has no matching UnaryOperator<Exp>");
        };
    }

    /**
     * Extracts the variable name from a string formatted as "${variableName}".
     * If it matches this format, it returns the substring between these markers, otherwise it throws an
     * {@code IllegalArgumentException}.
     *
     * @param variableReference The input string
     * @return The extracted variable name
     * @throws IllegalArgumentException if the input string does not match the format
     */
    static String extractVariableNameOrFail(String variableReference) {
        if (variableReference.startsWith("${") && variableReference.endsWith("}")) {
            return variableReference.substring(2, variableReference.length() - 1);
        }
        throw new IllegalArgumentException("Input string is not in the correct format");
    }

    /**
     * Overrides Exp type information for both the left and right {@link AbstractPart}s.
     * This method ensures that if a part's Exp type is not explicitly set, it attempts to infer and update
     * its type based on the opposite part of expression.
     *
     * @param left  The left {@link AbstractPart} operand, whose type information might be updated
     * @param right The right {@link AbstractPart} operand, whose type information might be updated
     * @throws AelParseException If either the left or right operand is {@code null}
     */
    static void overrideTypeInfo(AbstractPart left, AbstractPart right) {
        if (left == null) {
            throw new AelParseException("Unable to parse left operand");
        }
        if (right == null) {
            throw new AelParseException("Unable to parse right operand");
        }
        if (left.getPartType() == PLACEHOLDER_OPERAND || right.getPartType() == PLACEHOLDER_OPERAND) {
            return;
        }

        // Handle left part
        overrideTypes(left, right);
        // Handle right part
        overrideTypes(right, left);
    }

    /**
     * Overrides Exp type of the {@code left} {@link AbstractPart} based on the {@code right} {@link AbstractPart}.
     * This method handles different types of {@link AbstractPart} (e.g., {@code BIN_PART}, {@code PATH_OPERAND},
     * {@code EXPRESSION_CONTAINER}) and applies type overriding logic accordingly.
     *
     * @param left  The {@link AbstractPart} whose type might be overridden
     * @param right The {@link AbstractPart} used as a reference for type inference
     */
    private static void overrideTypes(AbstractPart left, AbstractPart right) {
        switch (left.getPartType()) {
            case BIN_PART -> overrideBinType((BinPart) left, right); // For example, in expression "$.intBin1 == 100"
            case PATH_OPERAND -> { // For example, in "$.listBin1.[0].get(return: EXISTS) == true"
                Path path = (Path) left;
                // Update the type of BinPart
                overrideBinType(path.getBasePath().getBinPart(), right);
                // Update the type of each CDT part
                for (AbstractPart cdtPart : path.getBasePath().getCdtParts()) {
                    overrideType(cdtPart, right);
                }
            }
            case EXPRESSION_CONTAINER -> { // For example, in "(5.2 + $.bananas) > 10.2"
                ExpressionContainer container = (ExpressionContainer) left;
                // Update the type of left part of container
                overrideTypeInfo(container.getLeft(), right);
                AbstractPart rightPart = container.getRight();
                // Update the type of right part of container
                if (rightPart != null) {
                    overrideTypeInfo(rightPart, right);
                }
            }
        }
    }

    /**
     * Overrides Exp type of the given {@link BinPart} based on the {@code right} {@link AbstractPart} if type is not
     * explicitly set already.
     *
     * @param binPart The {@link BinPart} whose type might be overridden
     * @param right   The {@link AbstractPart} used as a reference for type inference
     */
    private static void overrideBinType(BinPart binPart, AbstractPart right) {
        if (!binPart.isTypeExplicitlySet()) {
            overrideType(binPart, right);
        }
    }

    /**
     * Overrides Exp type of single {@link AbstractPart} based on the implicit type derived from {@code oppositePart}.
     * This method applies type overriding using implicit type detection.
     * It handles {@code BIN_PART} and other parts separately.
     *
     * @param part         The {@link AbstractPart} whose type needs to be overridden
     * @param oppositePart The {@link AbstractPart} used to determine the implicit type
     */
    private void overrideType(AbstractPart part, AbstractPart oppositePart) {
        // Override using Implicit type detection
        Exp.Type implicitType = partTypeToExpType.get(oppositePart.getPartType());

        if (part.getPartType() == BIN_PART) {
            if (implicitType != null) {
                ((BinPart) part).updateExp(implicitType);
            }
        } else { // CDT: ListPart or MapPart
            if (implicitType == null) {
                implicitType = TypeUtils.getDefaultType(part);
            }
            part.setExpType(implicitType);
        }
    }

    /**
     * Sets the logical bin type for a single expression container to {@link Exp.Type#BOOL}
     * if it represents a bin part.
     *
     * @param expr The {@link ExpressionContainer} to potentially update
     */
    static void logicalSetBinAsBooleanExpr(ExpressionContainer expr) {
        if (expr.getPartType() == BIN_PART) {
            ((BinPart) expr).updateExp(Exp.Type.BOOL);
        }
    }

    /**
     * Determines whether a child parse tree element at a specific index within a list
     * should be visited during tree traversal.
     *
     * @param i     The index of the child element
     * @param size  The total number of children in the list context
     * @param child The parse tree child element at the given index
     * @return {@code true} if the child should be visited as a list element, {@code false} otherwise
     */
    static boolean shouldVisitListElement(int i, int size, ParseTree child) {
        return size > 0 // size is not 0
                && i != 0 // not the first element ('[')
                && i != size - 1 // not the last element (']')
                && !child.getText().equals(","); // not a comma (list elements separator)
    }

    /**
     * Determines whether a child parse tree element at a specific index within a map
     * should be visited during tree traversal.
     *
     * @param i     The index of the child element
     * @param size  The total number of children in the map context
     * @param child The parse tree child element at the given index
     * @return {@code true} if the child should be visited as a map element, {@code false} otherwise
     */
    static boolean shouldVisitMapElement(int i, int size, ParseTree child) {
        return size > 0 // size is not 0
                && i != 0 // not the first element ('{')
                && i != size - 1 // not the last element ('}')
                && !child.getText().equals(":") // not a colon (map key and value separator)
                && !child.getText().equals(","); // not a comma (map pairs separator)
    }

    /**
     * Creates an expression for comparing a bin with another operand.
     *
     * @param binPart     The bin part
     * @param anotherPart The other operand to compare with
     * @param operator    The binary operator to apply
     * @param isBinLeft   Whether the bin is on the left side of the comparison
     * @return The resulting expression
     * @throws AelParseException if the operand type is not supported
     */
    private static Exp getExpBinComparison(BinPart binPart, AbstractPart anotherPart,
                                           BinaryOperator<Exp> operator, boolean isBinLeft) {
        Exp binExp = Exp.bin(binPart.getBinName(), binPart.getExpType());
        Exp anotherExp = switch (anotherPart.getPartType()) {
            case INT_OPERAND -> {
                validateComparableTypes(binPart.getExpType(), Exp.Type.INT);
                yield anotherPart.getExp();
            }
            case FLOAT_OPERAND -> {
                validateComparableTypes(binPart.getExpType(), Exp.Type.FLOAT);
                yield anotherPart.getExp();
            }
            case BOOL_OPERAND -> {
                validateComparableTypes(binPart.getExpType(), Exp.Type.BOOL);
                yield anotherPart.getExp();
            }
            case STRING_OPERAND -> handleStringOperandComparison(binPart, (StringOperand) anotherPart);
            case BLOB_OPERAND -> {
                validateComparableTypes(binPart.getExpType(), Exp.Type.BLOB);
                yield anotherPart.getExp();
            }
            case METADATA_OPERAND -> {
                // Handle metadata comparison - type determined by metadata function
                Exp.Type binType = Exp.Type.valueOf(((MetadataOperand) anotherPart).getMetadataType().toString());
                binExp = Exp.bin(binPart.getBinName(), binType);
                yield anotherPart.getExp();
            }
            case EXPRESSION_CONTAINER -> {
                Exp.Type resolvedType = resolveExpType(anotherPart);
                if (resolvedType != null) {
                    validateComparableTypes(binPart.getExpType(), resolvedType);
                }
                yield anotherPart.getExp();
            }
            case PATH_OPERAND, VARIABLE_OPERAND, UNKNOWN_OPERAND -> anotherPart.getExp();
            case BIN_PART -> {
                // Both are bin parts
                validateComparableTypes(binPart.getExpType(), anotherPart.getExpType());
                yield anotherPart.getExp();
            }
            case LIST_OPERAND -> {
                validateComparableTypes(binPart.getExpType(), Exp.Type.LIST);
                yield anotherPart.getExp();
            }
            case MAP_OPERAND -> {
                validateComparableTypes(binPart.getExpType(), Exp.Type.MAP);
                yield anotherPart.getExp();
            }
            default ->
                    throw new AelParseException("Operand type not supported: %s".formatted(anotherPart.getPartType()));
        };

        return isBinLeft ? operator.apply(binExp, anotherExp) : operator.apply(anotherExp, binExp);
    }

    /**
     * Handles string operand comparison with type validation and blob handling.
     *
     * @param binPart       The {@link BinPart} involved in the comparison
     * @param stringOperand The {@link StringOperand} involved in the comparison
     * @return The {@link Exp} generated from the {@link StringOperand}
     * @throws AelParseException if type validation fails (e.g., comparing a non-string/blob bin with a String)
     */
    private static Exp handleStringOperandComparison(BinPart binPart, StringOperand stringOperand) {
        boolean isBlobType = binPart.getExpType() != null && binPart.getExpType().equals(Exp.Type.BLOB);
        if (isBlobType) {
            // Handle base64 blob comparison
            validateComparableTypes(binPart.getExpType(), Exp.Type.BLOB);
            stringOperand.setBlob(true);
        } else {
            // Handle regular string comparison
            validateComparableTypes(binPart.getExpType(), Exp.Type.STRING);
        }
        return stringOperand.getExp();
    }

    /**
     * Creates an expression for comparing a bin on the left with an operand on the right.
     *
     * @param left     The {@link BinPart} on the left side of the comparison
     * @param right    The {@link AbstractPart} on the right side of the comparison
     * @param operator The binary operator to apply
     * @return The resulting {@link Exp} for the comparison
     * @throws AelParseException if an unsupported operand type is encountered or type validation fails
     */
    private static Exp getExpLeftBinTypeComparison(BinPart left, AbstractPart right, BinaryOperator<Exp> operator) {
        return getExpBinComparison(left, right, operator, true);
    }

    /**
     * Creates an expression for comparing an operand on the left with a bin on the right.
     *
     * @param left     The {@link AbstractPart} on the left side of the comparison
     * @param right    The {@link BinPart} on the right side of the comparison
     * @param operator The binary operator to apply
     * @return The resulting {@link Exp} for the comparison
     * @throws AelParseException if an unsupported operand type is encountered or type validation fails
     */
    private static Exp getExpRightBinTypeComparison(AbstractPart left, BinPart right, BinaryOperator<Exp> operator) {
        return getExpBinComparison(right, left, operator, false);
    }

    /**
     * Extracts the value of a specific parameter from a Path Function parameter context.
     *
     * @param paramCtx  The parse tree context for the Path Function parameter
     * @param paramName The name of the parameter to extract the value for
     * @return The value of the specified parameter if found and matches the name, otherwise {@code null}.
     */
    static String getPathFunctionParam(ConditionParser.PathFunctionParamContext paramCtx, String paramName) {
        String paramValue = null;
        if (paramCtx.pathFunctionParamName() != null) {
            String paramNameText = paramCtx.pathFunctionParamName().getText();
            String paramNameValue = paramCtx.pathFunctionParamValue().getText();
            if (paramNameText.equalsIgnoreCase(paramName)) {
                paramValue = paramNameValue;
            }
        }
        return paramValue;
    }

    /**
     * Determines the arithmetic term type based on the operation type and whether it is the left or right operand.
     *
     * @param operationType The type of the arithmetic operation
     * @param isLeftTerm    {@code true} if the term is the left operand, {@code false} if it's the right
     * @return The corresponding {@link ArithmeticTermType}
     * @throws NoApplicableFilterException if the operation type is not supported for determining arithmetic term type
     */
    private static ArithmeticTermType getFilterTermType(ExprPartsOperation operationType, boolean isLeftTerm) {
        return switch (operationType) {
            case ADD -> ADDEND;
            case SUB -> isLeftTerm ? SUBTR : MIN;
            case DIV -> isLeftTerm ? DIVIDEND : DIVISOR;
            case MUL -> isLeftTerm ? MULTIPLICAND : MULTIPLIER;
            default -> throw new NoApplicableFilterException(
                    "Operation type is not supported to get arithmetic term type: " + operationType);
        };
    }

    /**
     * This method determines the possible range of values for a bin that is either the dividend or the divisor
     * in a division operation, based on the values of the other operand and the filter operation type*
     *
     * @param left     The value of the left operand
     * @param right    The value of the right operand
     * @param type     The type of the filter operation
     * @param termType The {@link ArithmeticTermType} of the bin
     * @return A {@link Pair} representing the lower and upper bounds of the range for the bin.
     * A {@code null} value in the pair indicates no bound on that side
     * @throws NoApplicableFilterException if division by zero occurs or the term type is unsupported
     * @throws AelParseException           if undefined division (0/0) occurs
     */
    private static Pair<Long, Long> getLimitsForDivisionForFilter(long left, long right, FilterOperationType type,
                                                                  ArithmeticTermType termType) {
        // Prevent division by zero
        if (right == 0) {
            throw new NoApplicableFilterException("Cannot divide by zero");
        }

        return switch (termType) {
            case DIVIDEND -> LimitsForBinDividend(left, right, type);
            case DIVISOR -> getLimitsForBinDivisor(left, right, type);
            default -> throw new UnsupportedOperationException("Unsupported term type for division: " + termType);
        };
    }

    /**
     * This method determines the possible range of values for a bin when it is the dividend in a division operation,
     * based on the value of the divisor (right operand) and the filter operation type.
     *
     * @param left          The value of the dividend (the bin's value)
     * @param right         The value of the divisor
     * @param operationType The type of the filter operation
     * @return A {@link Pair} representing the lower and upper bounds of the range for the bin.
     * A {@code null} value in the pair indicates no bound on that side
     * @throws AelParseException if undefined division (0/0) occurs or if the operation type is not supported
     */
    private static Pair<Long, Long> LimitsForBinDividend(
            long left, long right, FilterOperationType operationType
    ) {
        if (left > 0 && right > 0) {
            // both operands are positive
            return getLimitsForBinDividendWithLeftNumberPositive(operationType, left, right);
        } else if (left == 0 && right == 0) {
            throw new AelParseException("Undefined division for 0 / 0");
        } else if (left < 0 && right < 0) {
            // both operands are negative
            return getLimitsForBinDividendWithLeftNumberNegative(operationType, left, right);
        } else if (left > 0 && right < 0) {
            // left positive, right negative
            return getLimitsForBinDividendWithLeftNumberPositive(operationType, left, right);
        } else if (right > 0 && left < 0) {
            // left negative, right positive
            return getLimitsForBinDividendWithLeftNumberNegative(operationType, left, right);
        } else if (left != 0) {
            throw new AelParseException("Division by zero is not allowed");
        } else {
            return new Pair<>(null, null);
        }
    }

    /**
     * Calculates the range limits for a bin that is the dividend when the left number is negative.
     *
     * @param operationType The type of the filter operation
     * @param left          The value of the dividend
     * @param right         The value of the divisor
     * @return A {@link Pair} representing the lower and upper bounds of the range for the bin
     * @throws AelParseException if the operation type is not supported for division
     */
    private static Pair<Long, Long> getLimitsForBinDividendWithLeftNumberNegative(
            FilterOperationType operationType, long left, long right
    ) {
        return switch (operationType) {
            case GT -> new Pair<>(Long.MIN_VALUE, left * right - 1);
            case GTEQ -> new Pair<>(Long.MIN_VALUE, left * right);
            case LT -> new Pair<>(left * right + 1, Long.MAX_VALUE);
            case LTEQ -> new Pair<>(left * right, Long.MAX_VALUE);
            default -> throw new AelParseException("OperationType not supported for division: " + operationType);
        };
    }

    /**
     * Calculates the range limits for a bin that is the dividend when the left number is positive.
     *
     * @param operationType The type of the filter operation
     * @param left          The value of the dividend
     * @param right         The value of the divisor
     * @return A {@link Pair} representing the lower and upper bounds of the range for the bin
     * @throws AelParseException if the operation type is not supported for division
     */
    private static Pair<Long, Long> getLimitsForBinDividendWithLeftNumberPositive(
            FilterOperationType operationType, long left, long right
    ) {
        return switch (operationType) {
            case GT -> new Pair<>(left * right + 1, Long.MAX_VALUE);
            case GTEQ -> new Pair<>(left * right, Long.MAX_VALUE);
            case LT -> new Pair<>(Long.MIN_VALUE, left * right - 1);
            case LTEQ -> new Pair<>(Long.MIN_VALUE, left * right);
            default -> throw new AelParseException("OperationType not supported for division: " + operationType);
        };
    }

    /**
     * Calculates the range limits for a bin that is the divisor in a division operation.
     *
     * @param left          The value of the dividend
     * @param right         The value of the divisor
     * @param operationType The type of the filter operation
     * @return A {@link Pair} representing the lower and upper bounds of the range for the bin.
     * A {@code null} value in the pair indicates no bound on that side
     * @throws AelParseException if division by zero occurs or if the operation type is not supported
     */
    private static Pair<Long, Long> getLimitsForBinDivisor(long left, long right, FilterOperationType operationType) {
        if (left > 0 && right > 0) {
            // both operands are positive
            return switch (operationType) {
                case GT -> new Pair<>(1L, getClosestLongToTheLeft((float) left / right));
                case GTEQ -> new Pair<>(1L, left / right);
                case LT, LTEQ -> new Pair<>(null, null);
                default -> throw new AelParseException("OperationType not supported for division: " + operationType);
            };
        } else if (left == 0 && right == 0) {
            throw new AelParseException("Cannot divide by zero");
        } else if (left < 0 && right < 0) {
            // both operands are negative
            return switch (operationType) {
                case GT, GTEQ -> new Pair<>(null, null);
                case LT -> new Pair<>(1L, getClosestLongToTheLeft((float) left / right));
                case LTEQ -> new Pair<>(1L, left / right);
                default -> throw new AelParseException("OperationType not supported for division: " + operationType);
            };
        } else if (left > 0 && right < 0) {
            // left positive, right negative
            return switch (operationType) {
                case GT, GTEQ -> new Pair<>(null, null);
                case LT -> new Pair<>(getClosestLongToTheRight((float) left / right), -1L);
                case LTEQ -> new Pair<>(left / right, -1L);
                default -> throw new AelParseException("OperationType not supported for division: " + operationType);
            };
        } else if (right > 0 && left < 0) {
            // right positive, left negative
            return switch (operationType) {
                case GT -> new Pair<>(getClosestLongToTheRight((float) left / right), -1L);
                case GTEQ -> new Pair<>(left / right, -1L);
                case LT, LTEQ -> new Pair<>(null, null);
                default -> throw new AelParseException("OperationType not supported for division: " + operationType);
            };
        } else if (left != 0) {
            throw new AelParseException("Division by zero is not allowed");
        } else {
            return new Pair<>(null, null);
        }
    }

    /**
     * Generates a {@link Filter} for a division operation based on the calculated value range and operation type.
     *
     * @param binName The name of the bin to filter on
     * @param value   A {@link Pair} representing the lower and upper bounds of the acceptable range for the bin.
     *                A {@code null} value in the pair indicates no bound on that side
     * @param type    The type of the filter operation
     * @return A {@link Filter} representing the condition
     * @throws AelParseException if the operation type is not supported for generating a filter
     */
    private static Filter getFilterForDivOrFail(String binName, Pair<Long, Long> value, FilterOperationType type) {
        // Based on the operation type, generate the appropriate filter range
        return switch (type) {
            case GT, GTEQ, LT, LTEQ -> Filter.range(binName, value.a, value.b);  // Range from 1 to value - 1
            case EQ -> Filter.equal(binName, value.a);  // Exact match for equality case
            default -> throw new AelParseException("OperationType not supported for division: " + type);
        };
    }

    /**
     * Creates a Filter based on a bin and an operand.
     *
     * @param bin     The bin part
     * @param operand The operand part
     * @param type    The filter operation type
     * @return The appropriate {@link Filter}
     * @throws NoApplicableFilterException if no appropriate filter can be created
     */
    private static Filter getFilterFromBin(BinPart bin, AbstractPart operand, FilterOperationType type) {
        validateOperands(bin, operand);
        return doGetFilterFromBin(bin, operand, type, null);
    }

    /**
     * Creates a Filter based on a bin and an operand, applies an array of {@link CTX} if provided.
     *
     * @param bin     The bin part
     * @param operand The operand part
     * @param type    The filter operation type
     * @param ctx     Array of {@link CTX} objects representing context, can be null
     * @return The appropriate {@link Filter}
     * @throws NoApplicableFilterException if no appropriate filter can be created
     */
    private static Filter doGetFilterFromBin(BinPart bin, AbstractPart operand, FilterOperationType type,
                                             CTX[] ctx) {
        String binName = bin.getBinName();
        return switch (operand.getPartType()) {
            case INT_OPERAND -> {
                validateComparableTypes(bin.getExpType(), Exp.Type.INT);
                yield getFilterForArithmeticOrFail(binName, ((IntOperand) operand).getValue(), type, ctx);
            }
            case STRING_OPERAND -> handleStringOperand(bin, binName, ((StringOperand) operand).getValue(), type, ctx);
            case BLOB_OPERAND -> {
                validateComparableTypes(bin.getExpType(), Exp.Type.BLOB);
                if (type != FilterOperationType.EQ) {
                    throw new NoApplicableFilterException(
                            "BLOB filter supports only equality comparison");
                }
                yield Filter.equal(binName, ((BlobOperand) operand).getValue(), ctx);
            }
            default -> throw new NoApplicableFilterException(
                    "Operand type not supported: %s".formatted(operand.getPartType()));
        };
    }

    /**
     * Creates a Filter based on a path and an operand.
     *
     * @param path    The path part
     * @param operand The operand part
     * @param type    The filter operation type
     * @return The appropriate {@link Filter}
     * @throws NoApplicableFilterException if no appropriate filter can be created
     */
    private static Filter getFilterFromPath(Path path, AbstractPart operand, FilterOperationType type) {
        validateOperands(path, operand);
        BinPart binPart = path.getBasePath().getBinPart();
        return doGetFilterFromBin(binPart, operand, type, path.getCtx());
    }

    /**
     * This method is used to generate a {@link Filter} when one of the operands is a {@link BinPart}
     * and the other is a {@link StringOperand}. It currently only supports equality (`EQ`) comparisons.
     * It handles both regular strings and base64 encoded BLOBs.
     *
     * @param bin          The {@link BinPart} involved in the filter
     * @param binName      The name of the bin
     * @param operandValue The value of {@link StringOperand} involved in the filter
     * @param type         The type of the filter operation (must be {@link FilterOperationType#EQ})
     * @param ctx          Array of {@link CTX} objects representing CDT context, can be null
     * @return An Aerospike {@link Filter} for the string or blob comparison
     * @throws NoApplicableFilterException if the filter operation type is not equality
     * @throws AelParseException           if type validation fails or base64 decoding fails
     */
    private static Filter handleStringOperand(BinPart bin, String binName, String operandValue,
                                              FilterOperationType type, CTX[] ctx) {
        if (type != FilterOperationType.EQ) {
            throw new NoApplicableFilterException("Only equality comparison is supported for string operands");
        }

        // Handle BLOB type
        if (bin.getExpType() != null && bin.getExpType().equals(Exp.Type.BLOB)) {
            validateComparableTypes(bin.getExpType(), Exp.Type.BLOB);
            byte[] value = Base64.getDecoder().decode(operandValue);
            return Filter.equal(binName, value, ctx);
        }

        // Handle STRING type
        validateComparableTypes(bin.getExpType(), Exp.Type.STRING);
        return Filter.equal(binName, operandValue, ctx);
    }

    /**
     * Creates a Filter based on two operands and a filter operation type.
     *
     * @param left  The left operand
     * @param right The right operand
     * @param type  The filter operation type
     * @return The appropriate Filter, or null if no filter can be created
     * @throws AelParseException if operands are invalid
     */
    private static Filter getFilterOrNull(AbstractPart left, AbstractPart right, FilterOperationType type) {
        validateOperands(left, right);

        // Handle bin operands
        if (left.getPartType() == BIN_PART) {
            return getFilterFromBin((BinPart) left, right, type);
        } else if (left.getPartType() == PATH_OPERAND) {
            return getFilterFromPath((Path) left, right, type);
        }
        if (right.getPartType() == BIN_PART) {
            return getFilterFromBin((BinPart) right, left, invertType(type));
        } else if (right.getPartType() == PATH_OPERAND) {
            return getFilterFromPath((Path) right, left, invertType(type));
        }

        // Handle expressions
        if (left.getPartType() == EXPRESSION_CONTAINER) {
            return handleExpressionOperand((ExpressionContainer) left, right, type);
        }

        if (right.getPartType() == EXPRESSION_CONTAINER) {
            return handleExpressionOperand((ExpressionContainer) right, left, type);
        }

        return null;
    }

    /**
     * This method is used when one of the operands is an {@link ExpressionContainer}.
     * It recursively processes the nested expression to determine if a filter can be generated from it in combination
     * with the {@code otherOperand} and the overall {@code type} of the filter operation.
     *
     * @param expr         The {@link ExpressionContainer} operand
     * @param otherOperand The other operand in the filter condition
     * @param type         The type of the filter operation
     * @return A {@link Filter} if one can be generated from the nested expression, otherwise {@code null}
     * @throws AelParseException           if operands within the nested expression are null
     * @throws NoApplicableFilterException if the nested expression structure is not supported for filtering
     */
    private static Filter handleExpressionOperand(ExpressionContainer expr, AbstractPart otherOperand,
                                                  FilterOperationType type) {
        AbstractPart exprLeft = expr.getLeft();
        AbstractPart exprRight = expr.getRight();
        ExprPartsOperation operation = expr.getOperationType();

        validateOperands(exprLeft, exprRight);

        return getFilterFromExpressionOrNull(exprLeft, exprRight, operation, otherOperand, type);
    }

    /**
     * Creates a secondary index {@link Filter} based on an expression and an external operand.
     * The method examines the structure of the nested expression and attempts to generate a {@link Filter}
     * by combining it with the {@code externalOperand} and the overall {@code type} of the filter operation.
     * It specifically looks for cases where a bin is involved in an arithmetic expression with an external operand.
     *
     * @param exprLeft        The left part of an expression
     * @param exprRight       The right part of an expression
     * @param operationType   The operation type of the expression
     * @param externalOperand The operand outside the expression
     * @param type            The type of the overall filter operation
     * @return A {@link Filter} if one can be generated, otherwise {@code null}
     * @throws NoApplicableFilterException if the expression structure is not supported for filtering
     */
    private static Filter getFilterFromExpressionOrNull(AbstractPart exprLeft, AbstractPart exprRight,
                                                        ExprPartsOperation operationType,
                                                        AbstractPart externalOperand, FilterOperationType type) {
        // Handle bin on left side
        if (exprLeft.getPartType() == BIN_PART) {
            return handleBinArithmeticExpression((BinPart) exprLeft, exprRight, externalOperand,
                    operationType, type, true);
        }

        // Handle bin on right side
        if (exprRight.getPartType() == BIN_PART) {
            return handleBinArithmeticExpression((BinPart) exprRight, exprLeft, externalOperand,
                    operationType, type, false);
        }

        // Handle nested expressions
        if (exprLeft.getPartType() == EXPRESSION_CONTAINER) {
            return getFilterOrNull(exprLeft, exprRight, type);
        }

        return null;
    }

    /**
     * This method is used when a secondary index {@link Filter} is being generated from an arithmetic
     * expression where one operand is a bin and the other is a literal value. It enforces
     * that both the literal operand and the external operand (from the overall filter condition)
     * must be integers for secondary index filtering.
     * It then calls{@link #applyFilterOperator} to generate the actual filter.
     *
     * @param bin             The {@link BinPart} involved in the arithmetic expression
     * @param operand         The other operand within the arithmetic expression (expected to be an integer)
     * @param externalOperand The operand from the overall filter condition (expected to be an integer)
     * @param operation       The type of the arithmetic operation
     * @param type            The type of the overall filter operation
     * @param isBinOnLeft       {@code true} if the bin is on the left side of the arithmetic operation, {@code false} otherwise
     * @return A {@link Filter} for the arithmetic condition
     * @throws NoApplicableFilterException if operands are not integers or if the operation is not supported
     * @throws AelParseException           if type validation fails
     */
    private static Filter handleBinArithmeticExpression(BinPart bin, AbstractPart operand,
                                                        AbstractPart externalOperand,
                                                        ExprPartsOperation operation,
                                                        FilterOperationType type, boolean isBinOnLeft) {
        // Only support integer arithmetic in filters
        if (operand.getPartType() != AbstractPart.PartType.INT_OPERAND || externalOperand.getPartType() != AbstractPart.PartType.INT_OPERAND) {
            throw new NoApplicableFilterException(
                    "Only integer operands are supported in arithmetic filter expressions");
        }

        validateComparableTypes(bin.getExpType(), Exp.Type.INT);

        IntOperand firstOperand = (IntOperand) operand;
        IntOperand secondOperand = (IntOperand) externalOperand;

        return applyFilterOperator(bin.getBinName(), firstOperand, secondOperand,
                operation, type, getFilterTermType(operation, isBinOnLeft));
    }

    /**
     * Validates that both left and right operands are not null.
     * This is a basic validation helper used to ensure that essential parts
     * of an expression are present before attempting to process them.
     *
     * @param left  The left {@link AbstractPart}
     * @param right The right {@link AbstractPart}
     * @throws AelParseException if either the left or right operand is null
     */
    private static void validateOperands(AbstractPart left, AbstractPart right) {
        if (left == null) {
            throw new AelParseException("Left operand cannot be null");
        }
        if (right == null) {
            throw new AelParseException("Right operand cannot be null");
        }
    }

    /**
     * This method handles arithmetic operations between two integer operands and converts the result
     * into an appropriate secondary index {@link Filter} based on the filter operation type.
     *
     * @param binName       The name of the bin to apply the filter to
     * @param leftOperand   The left {@link IntOperand}
     * @param rightOperand  The right {@link IntOperand}
     * @param operationType The type of the arithmetic operation
     * @param type          The type of the filter operation
     * @param termType      The {@link ArithmeticTermType} of the bin
     * @return A secondary index {@link Filter}
     * @throws NoApplicableFilterException if the operation is not supported by secondary index filters,
     *                                     division by zero occurs, or the calculated range is invalid
     * @throws AelParseException           if undefined division (0/0) occurs or other issues arise
     */
    private static Filter applyFilterOperator(String binName, IntOperand leftOperand, IntOperand rightOperand,
                                              ExprPartsOperation operationType, FilterOperationType type,
                                              ArithmeticTermType termType) {
        long leftValue = leftOperand.getValue();
        long rightValue = rightOperand.getValue();
        float value;
        if (Objects.requireNonNull(operationType) == ADD) {
            value = (float) rightValue - leftValue;
        } else if (operationType == SUB) {
            value = switch (termType) {
                case SUBTR -> rightValue + leftValue;
                case MIN -> {
                    type = invertType(type);
                    yield leftValue - rightValue;
                }
                default -> throw new IllegalStateException("Unexpected term type: " + termType);
            };
        } else if (operationType == DIV) {
            Pair<Long, Long> valueForDiv = getLimitsForDivisionForFilter(leftValue, rightValue, type, termType);
            if (valueForDiv.a == null
                    || valueForDiv.b == null
                    || valueForDiv.a > valueForDiv.b
                    || (valueForDiv.a == 0 && valueForDiv.b == 0)) {
                throw new NoApplicableFilterException("The operation is not supported by secondary index filter");
            }
            return getFilterForDivOrFail(binName, valueForDiv, type);
        } else if (operationType == MUL) {
            if (leftValue <= 0) {
                if (leftValue == 0) throw new NoApplicableFilterException("Cannot divide by zero");
                type = invertType(type);
            }
            float val = (float) rightValue / leftValue;
            return getFilterForArithmeticOrFail(binName, val, type, null);
        } else {
            throw new UnsupportedOperationException("Not supported");
        }
        return getFilterForArithmeticOrFail(binName, value, type, null);
    }

    /**
     * Generates a {@link Filter} for an arithmetic operation involving a bin and a value.
     *
     * @param binName The name of the bin to filter on
     * @param value   The calculated value from the arithmetic operation
     * @param type    The type of the filter operation
     * @param ctx     Array of {@link CTX} representing context, can be null
     * @return A {@link Filter} representing the condition
     * @throws NoApplicableFilterException if the operation type is not supported for secondary index filter
     */
    private static Filter getFilterForArithmeticOrFail(String binName, float value, FilterOperationType type, CTX[] ctx) {
        return switch (type) {
            // "$.intBin1 > 100" and "100 < $.intBin1" represent the same Filter
            case GT -> Filter.range(binName, getClosestLongToTheRight(value), Long.MAX_VALUE, ctx);
            case GTEQ -> Filter.range(binName, (long) value, Long.MAX_VALUE, ctx);
            case LT -> Filter.range(binName, Long.MIN_VALUE, getClosestLongToTheLeft(value), ctx);
            case LTEQ -> Filter.range(binName, Long.MIN_VALUE, (long) value, ctx);
            case EQ -> Filter.equal(binName, (long) value, ctx);
            default ->
                    throw new NoApplicableFilterException("The operation is not supported by secondary index filter");
        };
    }

    /**
     * Finds the closest long integer to the left of a given float value.
     *
     * @param value The float value
     * @return The closest long integer to the left
     */
    private static long getClosestLongToTheLeft(float value) {
        // Get the largest integer less than or equal to the float
        long flooredValue = (long) Math.floor(value);

        // If the float is a round number, subtract 1
        if (value == flooredValue) {
            return flooredValue - 1;
        }
        return flooredValue;
    }

    /**
     * Finds the closest long integer to the right of a given float value.
     *
     * @param value The float value
     * @return The closest long integer to the right
     */
    private static long getClosestLongToTheRight(float value) {
        // Get the smallest integer greater than or equal to the float
        long ceiledValue = (long) Math.ceil(value);

        // If the float is a round number, add 1
        if (value == ceiledValue) {
            return ceiledValue + 1;
        }

        return ceiledValue;
    }

    /**
     * This method provides the inverse operation for {@link FilterOperationType} comparison types.
     * For other Filter operation types, it returns the original type.
     *
     * @param type Filter operation type to invert
     * @return The inverted {@link FilterOperationType}
     */
    private static FilterOperationType invertType(FilterOperationType type) {
        return switch (type) {
            case GT -> FilterOperationType.LT;
            case GTEQ -> FilterOperationType.LTEQ;
            case LT -> FilterOperationType.GT;
            case LTEQ -> FilterOperationType.GTEQ;
            default -> type;
        };
    }

    /**
     * Builds a secondary index {@link Filter} and a filter {@link Exp} for a given {@link ExpressionContainer}.
     * This is the main entry point for enriching the parsed expression tree with query filters.
     *
     * @param expr              The {@link ExpressionContainer} representing the expression tree
     * @param placeholderValues The {@link PlaceholderValues} to match with placeholders by index
     * @param indexes           A map of available secondary indexes, keyed by bin name
     * @param preferredBin      Optional bin name hint; if non-null the selection algorithm prefers
     *                          an index on this bin, falling back to cardinality-based selection
     *                          when the hint cannot be applied
     * @return The updated {@link ExpressionContainer} with the generated {@link Filter} and {@link Exp}.
     * Either of them can be null if there is no suitable filter
     */
    public static AbstractPart buildExpr(ExpressionContainer expr, PlaceholderValues placeholderValues,
                                         Map<String, List<Index>> indexes, String preferredBin) {
        if (placeholderValues != null) resolvePlaceholders(expr, placeholderValues);

        Filter secondaryIndexFilter = null;
        try {
            secondaryIndexFilter = getSIFilter(expr, indexes, preferredBin);
        } catch (NoApplicableFilterException e) {
            clearSecondaryIndexFilterFlag(expr);
        }
        expr.setFilter(secondaryIndexFilter);

        Exp exp = getFilterExp(expr);
        expr.setExp(exp);
        return expr;
    }

    /**
     * Recursively resolves all placeholders within an expression tree.
     * <p>
     * This method traverses the expression tree starting from the root {@link ExpressionContainer}.
     * For each node, it checks for structures like {@link ExpressionContainer},
     * {@link WhenStructure}, and {@link LetStructure} and replaces any found placeholders with their
     * corresponding values.
     * </p>
     *
     * @param expression        The root of the expression tree to traverse
     * @param placeholderValues An object storing placeholder indexes and their resolved values
     */
    private static void resolvePlaceholders(ExpressionContainer expression, PlaceholderValues placeholderValues) {
        Consumer<AbstractPart> exprContainersCollector = part -> {
            switch (part.getPartType()) {
                case EXPRESSION_CONTAINER -> replacePlaceholdersInExprContainer(part, placeholderValues);
                case WHEN_STRUCTURE -> replacePlaceholdersInWhenStructure(part, placeholderValues);
                case LET_STRUCTURE -> replacePlaceholdersInLetStructure(part, placeholderValues);
                case EXCLUSIVE_STRUCTURE -> replacePlaceholdersInExclusiveStructure(part, placeholderValues);
            }
        };
        traverseTree(expression, exprContainersCollector, null);
    }

    /**
     * Replaces placeholders within a {@link LetStructure} object.
     * <p>
     * This method iterates through the operands of a given {@link LetStructure}. If an operand
     * is a {@link PlaceholderOperand}, it's resolved using the provided {@link PlaceholderValues}
     * and replaced with the resolved {@link AbstractPart}.
     * </p>
     *
     * @param part              The {@link AbstractPart} representing the {@link LetStructure}
     * @param placeholderValues An object storing placeholder indexes and their resolved values
     */
    private static void replacePlaceholdersInLetStructure(AbstractPart part, PlaceholderValues placeholderValues) {
        LetStructure letStructure = (LetStructure) part;
        for (LetOperand subOperand : letStructure.getOperands()) {
            if (subOperand.getPart().getPartType() == PLACEHOLDER_OPERAND) {
                // Replace placeholder part with the resolved operand
                subOperand.setPart(((PlaceholderOperand) subOperand.getPart()).resolve(placeholderValues));
            }
        }
    }

    /**
     * Replaces placeholders within a {@link WhenStructure} object.
     * <p>
     * This method iterates through the operands of a {@link WhenStructure}. It identifies
     * and resolves {@link PlaceholderOperand}s, replacing them with their resolved values
     * from the {@link PlaceholderValues}. It also recursively calls
     * {@code replacePlaceholdersInExprContainer} for any nested
     * {@link ExpressionContainer}s.
     * </p>
     *
     * @param part              The {@link AbstractPart} representing the {@link WhenStructure}
     * @param placeholderValues An object storing placeholder indexes and their resolved values
     */
    private static void replacePlaceholdersInWhenStructure(AbstractPart part, PlaceholderValues placeholderValues) {
        WhenStructure whenStructure = (WhenStructure) part;
        List<AbstractPart> subOperands = new ArrayList<>(whenStructure.getOperands());
        boolean isResolved = false;
        for (AbstractPart subOperand : whenStructure.getOperands()) {
            if (subOperand.getPartType() == PLACEHOLDER_OPERAND) {
                PlaceholderOperand placeholder = (PlaceholderOperand) subOperand;
                // Replace placeholder with the resolved operand
                subOperands.set(subOperands.indexOf(subOperand), placeholder.resolve(placeholderValues));
                isResolved = true;
            } else if (subOperand.getPartType() == EXPRESSION_CONTAINER) {
                replacePlaceholdersInExprContainer(subOperand, placeholderValues);
            }
        }

        if (isResolved) whenStructure.setOperands(subOperands);
    }

    /**
     * Replaces placeholders within a {@link ExclusiveStructure} object.
     * <p>
     * This method iterates through the operands of a given {@link ExclusiveStructure}. If an operand
     * has a {@link PlaceholderOperand}, it's resolved using the provided {@link PlaceholderValues}
     * and replaced with the resolved {@link AbstractPart}.
     * </p>
     *
     * @param part              The {@link AbstractPart} representing the {@link ExclusiveStructure}
     * @param placeholderValues An object storing placeholder indexes and their resolved values
     */
    private static void replacePlaceholdersInExclusiveStructure(AbstractPart part, PlaceholderValues placeholderValues) {
        ExclusiveStructure exclStructure = (ExclusiveStructure) part;
        for (AbstractPart operand : exclStructure.getOperands()) {
            if (operand instanceof ExpressionContainer ec) {
                replacePlaceholdersInExprContainer(ec, placeholderValues);
            }
        }
    }

    /**
     * Replaces placeholders within an {@link ExpressionContainer}.
     * <p>
     * Both the left and right operands are checked independently, so both may be
     * placeholders and both will be resolved in a single pass (e.g. {@code ?0 == ?1}).
     * </p>
     * <p>
     * After resolution, operation-specific type inference is applied:
     * <ul>
     *   <li>For comparison operations (LT, LTEQ, GT, GTEQ, NOTEQ, EQ) —
     *       {@code overrideTypeInfo} reconciles operand types.</li>
     *   <li>For IN operations — the right placeholder is validated to be a {@link java.util.List}
     *       before resolution, then {@code validateListHomogeneity} enforces uniform element types
     *       and {@code inferBinTypeFromList} infers the left bin type.</li>
     * </ul>
     *
     * @param part              The {@link AbstractPart} representing the {@link ExpressionContainer}
     * @param placeholderValues An object storing placeholder indexes and their resolved values
     */
    private static void replacePlaceholdersInExprContainer(AbstractPart part, PlaceholderValues placeholderValues) {
        ExpressionContainer expr = (ExpressionContainer) part;
        boolean leftIsPlaceholder = expr.getLeft().getPartType() == PLACEHOLDER_OPERAND;
        boolean rightIsPlaceholder = !expr.isUnary() && expr.getRight().getPartType() == PLACEHOLDER_OPERAND;
        boolean isResolved = false;

        if (expr.getOperationType() == IN && rightIsPlaceholder) {
            validateInPlaceholderValue((PlaceholderOperand) expr.getRight(), placeholderValues);
        }

        if (leftIsPlaceholder) {
            PlaceholderOperand placeholder = (PlaceholderOperand) expr.getLeft();
            expr.setLeft(placeholder.resolve(placeholderValues));
            isResolved = true;
        }
        if (rightIsPlaceholder) {
            PlaceholderOperand placeholder = (PlaceholderOperand) expr.getRight();
            expr.setRight(placeholder.resolve(placeholderValues));
            isResolved = true;
        }

        if (isResolved && List.of(LT, LTEQ, GT, GTEQ, NOTEQ, EQ).contains(expr.getOperationType())) {
            overrideTypeInfo(expr.getLeft(), expr.getRight());
        }
        if (isResolved && expr.getOperationType() == IN) {
            Exp.Type inferredType = validateListHomogeneity(expr.getRight());
            inferBinTypeFromList(expr.getLeft(), inferredType);
        }
    }

    /**
     * Validates that a placeholder used as the right operand of an IN expression
     * resolves to a {@link java.util.List}. Called before placeholder resolution
     * so that the error message references the placeholder index.
     *
     * @throws AelParseException if the placeholder index is missing or the placeholder value is not a List
     */
    private static void validateInPlaceholderValue(PlaceholderOperand placeholder,
                                                   PlaceholderValues placeholderValues) {
        int index = placeholder.getIndex();
        Object value;
        try {
            value = placeholderValues.getValue(index);
        } catch (IllegalArgumentException e) {
            throw new AelParseException(e.getMessage(), e);
        }
        if (!(value instanceof List)) {
            throw new AelParseException(
                    "IN operation requires a List as the right operand for placeholder ?" + index);
        }
    }

    /**
     * Validates that all elements in a LIST_OPERAND are of the same type.
     * <p>
     * If the right operand is not a LIST_OPERAND, returns {@code null} (nothing to validate).
     * Empty or all-null lists also return {@code null} (no type can be inferred).
     *
     * @param right the right operand of the IN expression
     * @return the inferred element type, or {@code null} when validation is not applicable
     * @throws AelParseException if the list contains elements of different types
     */
    static Exp.Type validateListHomogeneity(AbstractPart right) {
        if (right.getPartType() != LIST_OPERAND) {
            return null;
        }
        return inferTypeFromListElements((ListOperand) right);
    }

    /**
     * Infers or validates the left BIN_PART's Exp type against the list element type.
     * <p>
     * If the left operand is not a BIN_PART or {@code inferredType} is {@code null},
     * this method is a no-op.
     * When the bin type is not explicitly set, it is updated to {@code inferredType};
     * when it is explicitly set, compatibility is validated via {@code validateComparableTypes}.
     *
     * @param left         the left operand of the IN expression
     * @param inferredType the element type inferred from the right list operand (may be {@code null})
     */
    static void inferBinTypeFromList(AbstractPart left, Exp.Type inferredType) {
        if (inferredType == null || left.getPartType() != BIN_PART) {
            return;
        }
        BinPart leftBin = (BinPart) left;
        if (!leftBin.isTypeExplicitlySet()) {
            leftBin.updateExp(inferredType);
        } else {
            validateComparableTypes(leftBin.getExpType(), inferredType);
        }
    }

    /**
     * Infer the Aerospike Exp.Type for a list operand by examining its elements.
     * <p>
     * Assumes/enforces that all non-null elements in the list are of the same
     * logical type. If heterogeneous element types are detected, a
     * {@link AelParseException} is thrown to avoid silent type mismatches.
     *
     * @return the inferred type, or {@code null} if the list is empty or contains only nulls
     *         (no type can be inferred, so homogeneity validation is intentionally skipped)
     */
    static Exp.Type inferTypeFromListElements(ListOperand listOperand) {
        List<Object> values = listOperand.getValue();
        if (values.isEmpty()) {
            return null;
        }
        Exp.Type inferredType = null;
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            Exp.Type currentType = inferElementType(value);
            if (currentType == null) {
                throw new AelParseException(
                        "Unsupported element type in IN list: " + value.getClass().getName());
            }
            if (inferredType == null) {
                inferredType = currentType;
            } else if (inferredType != currentType) {
                throw new AelParseException(
                        "IN list elements must all be of the same type; found "
                                + inferredType + " and " + currentType);
            }
        }
        return inferredType;
    }

    /**
     * Map a single Java object to the corresponding Aerospike Exp.Type.
     */
    private static Exp.Type inferElementType(Object element) {
        if (element instanceof String) {
            return Exp.Type.STRING;
        }
        if (element instanceof Boolean) {
            return Exp.Type.BOOL;
        }
        if (element instanceof Float || element instanceof Double) {
            return Exp.Type.FLOAT;
        }
        if (element instanceof Integer || element instanceof Long) {
            return Exp.Type.INT;
        }
        if (element instanceof List) {
            return Exp.Type.LIST;
        }
        if (element instanceof Map) {
            return Exp.Type.MAP;
        }
        if (element instanceof byte[]) {
            return Exp.Type.BLOB;
        }
        return null;
    }

    /**
     * Builds an array of {@link CTX} for a given path.
     * This is the main entry point for building context based on the parsed expression tree.
     *
     * @param path The {@link AbstractPart} representing the path
     * @return Array of {@link CTX} objects representing the context, or null
     * @throws UnsupportedOperationException If the given input expression is not a path, or if it has path function
     */
    public static CTX[] buildCtx(AbstractPart path) {
        if (path.getPartType() == BIN_PART) {
            // No nested context
            throw new UnsupportedOperationException("CDT context is not provided");
        }
        if (path.getPartType() != PATH_OPERAND) {
            throw new UnsupportedOperationException(
                    String.format("Unsupported input expression type '%s', please provide only path to convert to CTX[]",
                            path.getPartType())
            );
        }
        if (((Path) path).getPathFunction() != null) {
            throw new UnsupportedOperationException("Path function is unsupported, please provide only path to convert to CTX[]");
        }
        return path.getCtx();
    }

    /**
     * Returns the {@link Exp} generated for a given {@link ExpressionContainer}.
     *
     * @param expr The input {@link ExpressionContainer}
     * @return The corresponding {@link Exp}, or {@code null} if a secondary index filter was applied
     * or if there is no suitable filter
     */
    private static Exp getFilterExp(ExpressionContainer expr) {
        // Skip the expression already used in creating secondary index Filter
        if (expr.hasSecondaryIndexFilter()) return null;

        return switch (expr.getOperationType()) {
            case OR_STRUCTURE -> orStructureToExp(expr);
            case AND_STRUCTURE -> andStructureToExp(expr);
            case LET_STRUCTURE -> letStructureToExp(expr);
            case WHEN_STRUCTURE -> whenStructureToExp(expr);
            case EXCLUSIVE_STRUCTURE -> exclStructureToExp(expr);
            case MIN_FUNC -> variadicToExp(expr, Exp::min);
            case MAX_FUNC -> variadicToExp(expr, Exp::max);
            case FIND_BIT_LEFT, FIND_BIT_RIGHT -> binaryFunctionToExp(expr);
            default -> processExpression(expr);
        };
    }

    /**
     * Converts a variadic function expression (e.g., min, max) to Exp.
     * Extracts operands from the {@link FunctionArgs} wrapper and passes them as an array.
     */
    private static Exp variadicToExp(ExpressionContainer expr,
                                      Function<Exp[], Exp> expFactory) {
        FunctionArgs funcArgs = (FunctionArgs) expr.getLeft();
        List<AbstractPart> operands = funcArgs.getOperands();
        Exp[] exps = operands.stream()
                .map(VisitorUtils::getExp)
                .toArray(Exp[]::new);
        return expFactory.apply(exps);
    }

    /**
     * Converts a binary function expression to Exp, bypassing BIN_PART type validation.
     * Used only for findBitLeft/findBitRight, which take (int, bool) operands that
     * intentionally fail standard {@code validateComparableTypes} validation.
     */
    private static Exp binaryFunctionToExp(ExpressionContainer expr) {
        Exp leftExp = getExp(expr.getLeft());
        Exp rightExp = getExp(expr.getRight());
        BinaryOperator<Exp> operator = getExpOperator(expr.getOperationType());
        return operator.apply(leftExp, rightExp);
    }

    /**
     * Generates filter {@link Exp} for a LET structure {@link ExpressionContainer}.
     *
     * @param expr The {@link ExpressionContainer} representing LET structure
     * @return The resulting {@link Exp} expression
     */
    private static Exp letStructureToExp(ExpressionContainer expr) {
        List<Exp> expressions = new ArrayList<>();
        LetStructure letOperandsList = (LetStructure) expr.getLeft(); // extract unary Expr operand
        List<LetOperand> operands = letOperandsList.getOperands();
        for (LetOperand letOperand : operands) {
            if (!letOperand.isLastPart()) {
                expressions.add(Exp.def(letOperand.getString(), getExp(letOperand.getPart())));
            } else {
                // the last expression is the action (described after "then")
                expressions.add(getExp(letOperand.getPart()));
            }
        }
        return Exp.let(expressions.toArray(new Exp[0]));
    }

    /**
     * Generates filter {@link Exp} for a WHEN structure {@link ExpressionContainer}.
     *
     * @param expr The {@link ExpressionContainer} representing WHEN structure
     * @return The resulting {@link Exp} expression
     */
    private static Exp whenStructureToExp(ExpressionContainer expr) {
        List<Exp> expressions = new ArrayList<>();
        WhenStructure whenOperandsList = (WhenStructure) expr.getLeft(); // extract unary Expr operand
        List<AbstractPart> operands = whenOperandsList.getOperands();
        for (AbstractPart part : operands) {
            expressions.add(getExp(part));
        }
        return Exp.cond(expressions.toArray(new Exp[0]));
    }

    /**
     * Generates filter {@link Exp} for an EXCLUSIVE structure {@link ExpressionContainer}.
     *
     * @param expr The {@link ExpressionContainer} representing EXCLUSIVE structure
     * @return The resulting {@link Exp} expression
     */
    private static Exp exclStructureToExp(ExpressionContainer expr) {
        List<Exp> expressions = new ArrayList<>();
        ExclusiveStructure exclOperandsList = (ExclusiveStructure) expr.getLeft();
        for (AbstractPart part : exclOperandsList.getOperands()) {
            expressions.add(getExp(part));
        }
        return Exp.exclusive(expressions.toArray(new Exp[0]));
    }

    private static Exp orStructureToExp(ExpressionContainer expr) {
        List<Exp> expressions = new ArrayList<>();
        for (AbstractPart part : ((OrStructure) expr.getLeft()).getOperands()) {
            expressions.add(getExp(part));
        }
        return Exp.or(expressions.toArray(new Exp[0]));
    }

    /**
     * Generates filter {@link Exp} for an AND structure {@link ExpressionContainer}.
     *
     * @param expr The {@link ExpressionContainer} representing AND structure
     * @return The resulting {@link Exp} expression
     */
    private static Exp andStructureToExp(ExpressionContainer expr) {
        List<Exp> expressions = new ArrayList<>();
        for (AbstractPart part : ((AndStructure) expr.getLeft()).getOperands()) {
            Exp exp = getExp(part);
            if (exp != null) expressions.add(exp);
        }
        if (expressions.isEmpty()) {
            return null;
        } else if (expressions.size() > 1) {
            return Exp.and(expressions.toArray(new Exp[0]));
        }
        return expressions.get(0);
    }

    /**
     * Processes an {@link ExpressionContainer} to generate the corresponding Exp.
     *
     * @param expr The expression to process
     * @return The processed Exp
     * @throws AelParseException if left or right operands are null in a binary expression
     */
    private static Exp processExpression(ExpressionContainer expr) {
        AbstractPart left = getExistingPart(expr.getLeft(), "Unable to parse left operand");

        if (expr.getOperationType() == EXISTS) {
            if (left.getPartType() == BIN_PART) {
                return Exp.binExists(((BinPart) left).getBinName());
            }
            return processOperand(left);
        }

        // For unary expressions
        if (expr.isUnary()) {
            Exp operandExp = processOperand(left);
            if (operandExp == null) return null;

            UnaryOperator<Exp> operator = getUnaryExpOperator(expr.getOperationType());
            return operator.apply(operandExp);
        }

        // For binary expressions
        AbstractPart right = getExistingPart(expr.getRight(), "Unable to parse right operand");

        // IN operation: ListExp.getByValue(EXISTS, leftExp, rightExp)
        if (expr.getOperationType() == IN) {
            return buildInExpression(left, right);
        }

        rejectBlobArithmetic(left, right, expr.getOperationType());
        coerceStringToBlobIfNeeded(left, right);

        // Process operands
        Exp leftExp = processOperand(left);
        Exp rightExp = processOperand(right);

        // Special handling for BIN_PART
        if (left.getPartType() == BIN_PART) {
            return getExpLeftBinTypeComparison((BinPart) left, right, getExpOperator(expr.getOperationType()));
        } else if (right.getPartType() == BIN_PART) {
            return getExpRightBinTypeComparison(left, (BinPart) right, getExpOperator(expr.getOperationType()));
        }

        // Special handling for AND operation
        if (expr.getOperationType() == AND) {
            if (leftExp == null) return rightExp;
            if (rightExp == null) return leftExp;
        }

        // Validate operand type compatibility for non-BIN_PART operands.
        // Arithmetic operations should not involve STRING operands, and comparison
        // results of arithmetic expressions should be type-compatible.
        ExprPartsOperation opType = expr.getOperationType();
        if (NUMERIC_OPERATIONS.contains(opType)
                || isArithmeticExpressionContainer(left)
                || isArithmeticExpressionContainer(right)) {
            validateComparableTypes(resolveExpType(left), resolveExpType(right));
        }

        // Apply binary operator
        BinaryOperator<Exp> operator = getExpOperator(expr.getOperationType());
        return operator.apply(leftExp, rightExp);
    }

    private static final EnumSet<ExprPartsOperation> ARITHMETIC_OPERATIONS = EnumSet.of(
            ADD, SUB, MUL, DIV, MOD, POW
    );

    private static void rejectBlobArithmetic(AbstractPart left, AbstractPart right,
                                             ExprPartsOperation opType) {
        if (ARITHMETIC_OPERATIONS.contains(opType)
                && (resolveExpType(left) == Exp.Type.BLOB
                    || resolveExpType(right) == Exp.Type.BLOB)) {
            throw new AelParseException(
                    "BLOB type does not support arithmetic operations");
        }
    }

    private static void coerceStringToBlobIfNeeded(AbstractPart left, AbstractPart right) {
        if (isBlobPath(left) && right.getPartType() == STRING_OPERAND) {
            ((StringOperand) right).setBlob(true);
        } else if (isBlobPath(right) && left.getPartType() == STRING_OPERAND) {
            ((StringOperand) left).setBlob(true);
        }
    }

    private static boolean isBlobPath(AbstractPart part) {
        return part.getPartType() == PATH_OPERAND
                && part instanceof Path path
                && path.getPathFunction() != null
                && path.getPathFunction().getBinType() == Exp.Type.BLOB;
    }

    // Operations that always produce a FLOAT result. Used by resolveExpType.
    private static final EnumSet<ExprPartsOperation> FLOAT_RETURNING_OPERATIONS = EnumSet.of(
            POW, LOG, CEIL, FLOOR, TO_FLOAT
    );

    // Operations that always produce an INT result regardless of input operand type.
    // Used by resolveExpType. All members are also present in NUMERIC_OPERATIONS.
    private static final EnumSet<ExprPartsOperation> INT_RETURNING_OPERATIONS = EnumSet.of(
            TO_INT,
            INT_AND, INT_OR, INT_XOR, INT_NOT,
            L_SHIFT, R_SHIFT, LOGICAL_R_SHIFT,
            COUNT_ONE_BITS, FIND_BIT_LEFT, FIND_BIT_RIGHT
    );

    // All arithmetic/bitwise/cast operations. Used by isArithmeticExpressionContainer to decide
    // whether to trigger type-compatibility validation on comparison operands (e.g. detecting
    // "28.asFloat() == \"hello\"" as invalid). FLOAT_RETURNING_OPERATIONS and
    // INT_RETURNING_OPERATIONS are intentional subsets — members that appear in both sets are
    // caught first by the more-specific checks inside resolveExpType.
    private static final EnumSet<ExprPartsOperation> NUMERIC_OPERATIONS = EnumSet.of(
            ADD, SUB, MUL, DIV, MOD, POW, INT_XOR, INT_NOT, INT_AND, INT_OR,
            L_SHIFT, R_SHIFT, LOGICAL_R_SHIFT, ABS, CEIL, FLOOR, LOG,
            MIN_FUNC, MAX_FUNC, COUNT_ONE_BITS, FIND_BIT_LEFT, FIND_BIT_RIGHT,
            TO_INT, TO_FLOAT
    );

    /**
     * Resolves the effective {@link Exp.Type} of an {@link AbstractPart}.
     * <ul>
     *   <li>Uses the part's explicit type if set.</li>
     *   <li>Falls back to the {@code partTypeToExpType} map for literal operands.</li>
     *   <li>For arithmetic {@link ExpressionContainer} nodes, determines the result type based on
     *       the operation:
     *       <ul>
     *         <li>Always-FLOAT: {@code POW}, {@code LOG}, {@code CEIL}, {@code FLOOR},
     *             {@code TO_FLOAT}</li>
     *         <li>Always-INT: bitwise ops, shift ops, {@code TO_INT}, {@code COUNT_ONE_BITS},
     *             {@code FIND_BIT_LEFT}, {@code FIND_BIT_RIGHT}</li>
     *         <li>Type-propagating: {@code ABS}, {@code ADD}, {@code SUB}, {@code MUL},
     *             {@code DIV}, {@code MOD}, {@code MIN_FUNC}, {@code MAX_FUNC} — result type
     *             follows the left operand; falls back to {@code INT} when unknown.</li>
     *       </ul>
     *   </li>
     * </ul>
     *
     * @param part The {@link AbstractPart} whose type to resolve
     * @return The resolved {@link Exp.Type}, or {@code null} if the type cannot be determined
     */
    private static Exp.Type resolveExpType(AbstractPart part) {
        if (part.getExpType() != null) {
            return part.getExpType();
        }
        Exp.Type mapped = partTypeToExpType.get(part.getPartType());
        if (mapped != null) {
            return mapped;
        }
        if (part instanceof ExpressionContainer container && container.getOperationType() != null) {
            ExprPartsOperation op = container.getOperationType();
            if (FLOAT_RETURNING_OPERATIONS.contains(op)) {
                return Exp.Type.FLOAT;
            }
            if (INT_RETURNING_OPERATIONS.contains(op)) {
                return Exp.Type.INT;
            }
            if (isArithmeticExpressionContainer(part)) {
                // Type-propagating operations (ABS, ADD, SUB, MUL, DIV, MOD, MIN_FUNC, MAX_FUNC):
                // propagate FLOAT when the left operand is known to be FLOAT.
                // Otherwise fall back to INT: any arithmetic result is numeric (never STRING/BOOL),
                // so returning INT conservatively ensures string comparisons are rejected.
                if (container.getLeft() != null
                        && resolveExpType(container.getLeft()) == Exp.Type.FLOAT) {
                    return Exp.Type.FLOAT;
                }
                return Exp.Type.INT;
            }
        }
        return null;
    }

    /**
     * Builds an Exp for an IN expression using {@code ListExp.getByValue(EXISTS, ...)}.
     *
     * @param left  the value to search for
     * @param right the list to search in
     * @return an Exp that evaluates to true if the left value exists in the right list
     */
    private static Exp buildInExpression(AbstractPart left, AbstractPart right) {
        Exp leftExp = processOperand(left);
        Exp rightExp = processOperand(right);
        return ListExp.getByValue(ListReturnType.EXISTS, leftExp, rightExp);
    }

    private static boolean isArithmeticExpressionContainer(AbstractPart part) {
        return part instanceof ExpressionContainer container
                && container.getOperationType() != null
                && NUMERIC_OPERATIONS.contains(container.getOperationType());
    }

    /**
     * Ensures that an {@link AbstractPart} object is not null.
     * <p>
     * This is a utility method used to validate that the given {@link AbstractPart} exists. If the provided
     * {@code part} is {@code null}, it indicates a parsing error, and a {@link AelParseException}
     * is thrown with a custom error message. Otherwise, the method simply returns the non-null part.
     * </p>
     *
     * @param part   The part of the expression to be validated
     * @param errMsg The error message to be included in the exception if the part is null
     * @return The provided {@link AbstractPart}, if it is not null
     * @throws AelParseException if the provided {@code part} is {@code null}
     */
    private static AbstractPart getExistingPart(AbstractPart part, String errMsg) {
        if (part == null) {
            throw new AelParseException(errMsg);
        }
        return part;
    }


    /**
     * Processes an expression operand to generate its corresponding Aerospike {@link Exp}.
     * If the operand is an {@link ExpressionContainer}, it recursively calls {@link #getFilterExp(ExpressionContainer)}
     * to get the nested expression's {@link Exp}. Otherwise, it retrieves the {@link Exp} from the part itself.
     * The generated {@link Exp} is set back on the {@link AbstractPart}.
     *
     * @param part The operand to process
     * @return The processed Exp, or {@code null} if the part is null or represents
     * an expression container that resulted in a null Exp
     */
    private static Exp processOperand(AbstractPart part) {
        if (part == null) return null;

        Exp exp;
        if (part.getPartType() == EXPRESSION_CONTAINER) {
            exp = getFilterExp((ExpressionContainer) part);
        } else {
            exp = part.getExp();
        }
        part.setExp(exp);
        return exp;
    }

    /**
     * This method that retrieves the {@link Exp} associated with an {@link AbstractPart}.
     * If the part is an {@link ExpressionContainer}, it calls {@link #getFilterExp(ExpressionContainer)}
     * to get the nested expression's {@link Exp}. Otherwise, it returns the {@link Exp} stored
     * directly in the {@link AbstractPart}.
     *
     * @param part The {@link AbstractPart} for which to get the {@link Exp}
     * @return The corresponding {@link Exp} or {@code null}
     */
    private static Exp getExp(AbstractPart part) {
        if (part.getPartType() == EXPRESSION_CONTAINER) {
            return getFilterExp((ExpressionContainer) part);
        }
        return part.getExp();
    }

    /**
     * Attempts to generate a secondary index {@link Filter} for a given {@link ExpressionContainer}.
     * If the expression is not an OR operation (which is not supported
     * for secondary index filters), the method attempts to find the most suitable
     * expression within the tree to apply a filter based on index availability and cardinality.
     *
     * @param expr         The {@link ExpressionContainer} representing the expression tree
     * @param indexes      A map of available secondary indexes, keyed by bin name
     * @param preferredBin Optional bin name hint for index selection preference
     * @return A secondary index {@link Filter}, or {@code null} if no applicable filter can be generated
     * @throws NoApplicableFilterException if the expression operation type is not supported
     */
    private static Filter getSIFilter(ExpressionContainer expr, Map<String, List<Index>> indexes,
                                      String preferredBin) {
        if (expr.getOperationType() == OR) return null;

        ExpressionContainer chosenExpr = chooseExprForFilter(expr, indexes, preferredBin);
        if (chosenExpr == null) return null;

        return getFilterOrNull(
                chosenExpr.getLeft(),
                chosenExpr.getRight(),
                getFilterOperation(chosenExpr.getOperationType())
        );
    }

    /**
     * Recursively clears the {@code hasSecondaryIndexFilter} flag on an expression tree.
     * Called when SI filter generation fails ({@link NoApplicableFilterException}), so that
     * {@link #getFilterExp} does not skip subtrees that were optimistically marked during
     * {@link #chooseExprForFilter}.
     */
    private static void clearSecondaryIndexFilterFlag(ExpressionContainer expr) {
        expr.hasSecondaryIndexFilter(false);
        if (expr.getLeft() instanceof ExpressionContainer left) {
            clearSecondaryIndexFilterFlag(left);
        }
        if (expr.getRight() instanceof ExpressionContainer right) {
            clearSecondaryIndexFilterFlag(right);
        }
    }

    /**
     * Chooses the most suitable {@link ExpressionContainer} within a tree to apply a secondary index filter.
     * Identifies all potential expressions within the tree that could utilize a secondary index.
     * When a {@code preferredBin} hint is provided, candidates matching that bin are tried first;
     * if none match, falls back to all candidates. Selection within a candidate set uses the
     * largest cardinality, then alphabetical bin name as a tiebreaker. The chosen expression
     * is marked as having a secondary index filter applied.
     *
     * @param exprContainer The root {@link ExpressionContainer} of the expression tree
     * @param indexes       A map of available secondary indexes, keyed by bin name
     * @param preferredBin  Optional bin name hint for index selection preference
     * @return The chosen {@link ExpressionContainer} for secondary index filtering,
     * or {@code null} if no suitable expression is found
     */
    private static ExpressionContainer chooseExprForFilter(ExpressionContainer exprContainer,
                                                           Map<String, List<Index>> indexes,
                                                           String preferredBin) {
        if (indexes == null || indexes.isEmpty()) return null;

        Map<Integer, List<ExpressionContainer>> exprsPerCardinality =
                getExpressionsPerCardinality(exprContainer, indexes);

        if (preferredBin != null) {
            Map<Integer, List<ExpressionContainer>> hintedCandidates = filterByBin(exprsPerCardinality, preferredBin);
            ExpressionContainer hinted = selectByCardinalityThenAlpha(hintedCandidates);
            if (hinted != null) return hinted;
        }

        return selectByCardinalityThenAlpha(exprsPerCardinality);
    }

    /**
     * Filters a cardinality map to only include expressions whose bin matches the given name.
     */
    private static Map<Integer, List<ExpressionContainer>> filterByBin(
            Map<Integer, List<ExpressionContainer>> exprsPerCardinality, String binName) {
        Map<Integer, List<ExpressionContainer>> filtered = new HashMap<>();
        for (Map.Entry<Integer, List<ExpressionContainer>> entry : exprsPerCardinality.entrySet()) {
            List<ExpressionContainer> matching = entry.getValue().stream()
                    .filter(expr -> binName.equals(getBinPart(expr, 1).getBinName()))
                    .toList();
            if (!matching.isEmpty()) {
                filtered.put(entry.getKey(), matching);
            }
        }
        return filtered;
    }

    /**
     * Selects an expression from a cardinality map using highest cardinality first,
     * then alphabetical bin name as a tiebreaker. Marks the chosen expression with
     * {@code hasSecondaryIndexFilter(true)}.
     *
     * @return The chosen expression, or {@code null} if the map is empty
     */
    private static ExpressionContainer selectByCardinalityThenAlpha(
            Map<Integer, List<ExpressionContainer>> exprsPerCardinality) {
        if (exprsPerCardinality.isEmpty()) return null;

        List<ExpressionContainer> topExprs = exprsPerCardinality.entrySet().stream()
                .max(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .orElse(Collections.emptyList());

        if (topExprs.isEmpty()) return null;

        ExpressionContainer chosen = topExprs.size() > 1
                ? topExprs.stream()
                .min(Comparator.comparing(expr -> getBinPart(expr, 1).getBinName()))
                .orElse(null)
                : topExprs.get(0);

        if (chosen != null) {
            chosen.hasSecondaryIndexFilter(true);
        }
        return chosen;
    }

    /**
     * Collects all {@link ExpressionContainer}s within an expression tree that
     * correspond to a bin with a secondary index, grouped by the index's cardinality.
     *
     * @param exprContainer The root {@link ExpressionContainer} of the expression tree
     * @param indexes       A map of available secondary indexes, keyed by bin name
     * @return A map where keys are secondary index cardinalities (bin values ratio)
     * and values are lists of {@link ExpressionContainer}s associated with bins
     * having that cardinality
     */
    private static Map<Integer, List<ExpressionContainer>> getExpressionsPerCardinality(
            ExpressionContainer exprContainer, Map<String, List<Index>> indexes
    ) {
        Map<Integer, List<ExpressionContainer>> exprsPerCardinality = new HashMap<>();
        final BinPart[] binPartPrev = {null};
        Consumer<AbstractPart> exprsPerCardinalityCollector = part -> {
            if (part.getPartType() == EXPRESSION_CONTAINER) {
                ExpressionContainer expr = (ExpressionContainer) part;

                if (expr.getOperationType() == IN) return;

                BinPart binPart = getBinPart(expr, 2);

                if (binPart == null) return; // no bin found
                if (!binPart.equals(binPartPrev[0])) {
                    binPartPrev[0] = binPart;
                } else {
                    return; // the same bin
                }

                updateExpressionsPerCardinality(exprsPerCardinality, expr, binPart, indexes);
            }
        };
        traverseTree(exprContainer, exprsPerCardinalityCollector, null);
        return exprsPerCardinality;
    }

    /**
     * Updates a map of expression containers grouped by the cardinality of the indexes associated with an
     * expression container's bin part.
     * This method iterates through the indexes related to the provided {@code binPart}.
     * For each index that matches the expression type of the {@code binPart}, it adds the given {@code expr}
     * to the {@code exprsPerCardinality} map.
     *
     * @param exprsPerCardinality A map where keys are integer ratios (representing index cardinality)
     *                            and values are lists of {@link ExpressionContainer} objects.
     *                            The map is updated by this method
     * @param expr                The {@link ExpressionContainer} to be added to the appropriate list
     *                            within {@code exprsPerCardinality}
     * @param binPart             The {@link BinPart} associated with the expression, used to find
     *                            relevant indexes and determine the expression type
     * @param indexes             A map where keys are bin names and values are lists of
     *                            {@link Index} objects associated with that bin
     */
    private static void updateExpressionsPerCardinality(Map<Integer, List<ExpressionContainer>> exprsPerCardinality,
                                                        ExpressionContainer expr, BinPart binPart,
                                                        Map<String, List<Index>> indexes) {
        List<Index> indexesByBin = indexes.get(binPart.getBinName());
        if (indexesByBin == null || indexesByBin.isEmpty()) return;

        for (Index idx : indexesByBin) {
            // Iterate over all indexes for the same bin
            if (expTypeToIndexType.get(binPart.getExpType()) == idx.getIndexType()) {
                List<ExpressionContainer> exprsList = exprsPerCardinality.get(idx.getBinValuesRatio());
                if (exprsList != null) {
                    exprsList.add(expr);
                } else {
                    exprsList = new ArrayList<>();
                    exprsList.add(expr);
                }
                exprsPerCardinality.put(idx.getBinValuesRatio(), exprsList);
            }
        }
    }

    /**
     * The method traverses the expression tree starting from the given {@link ExpressionContainer},
     * searching for a {@link BinPart}. It limits the search depth and stops
     * traversing a branch if a logical expression (AND / OR) is found.
     *
     * @param expr  The {@link ExpressionContainer} to start searching from
     * @param depth The maximum depth to traverse
     * @return The first {@link BinPart} found within the specified depth, or {@code null} if none is found
     */
    private static BinPart getBinPart(ExpressionContainer expr, int depth) {
        final BinPart[] singleBinPartArray = {null};
        Consumer<AbstractPart> binPartRetriever = part -> {
            if (part.getPartType() == BIN_PART) {
                singleBinPartArray[0] = (BinPart) part;
            } else if (part.getPartType() == PATH_OPERAND) {
                singleBinPartArray[0] = ((Path) part).getBasePath().getBinPart();
            }
        };
        Predicate<AbstractPart> stopOnLogicalExpr = part -> {
            if (part.getPartType() == EXPRESSION_CONTAINER) {
                ExpressionContainer logicalExpr = (ExpressionContainer) part;
                if (logicalExpr.isExclFromSecondaryIndexFilter()) {
                    propagateExclFlag(logicalExpr.getLeft());
                    propagateExclFlag(logicalExpr.getRight());
                    return true;
                }
                if (logicalExpr.getOperationType() == AND) return true;
                if (logicalExpr.getOperationType() == OR) {
                    propagateExclFlag(logicalExpr.getLeft());
                    propagateExclFlag(logicalExpr.getRight());
                    return true;
                }
            }
            return false;
        };

        traverseTree(expr, binPartRetriever, depth, stopOnLogicalExpr);
        return singleBinPartArray[0];
    }

    private static void propagateExclFlag(AbstractPart part) {
        if (part != null && part.getPartType() == EXPRESSION_CONTAINER) {
            ((ExpressionContainer) part).isExclFromSecondaryIndexFilter(true);
        }
    }

    /**
     * Traverses the AbstractPart nodes tree and applies the visitor function to each node.
     * Uses a pre-order traversal (top-down, root-left-right).
     * The visitor function can be used to modify the node's state or to extract information.
     *
     * @param part          The current node being visited (start with the root)
     * @param visitor       The function to apply to each AbstractPart node
     * @param stopCondition The condition that causes stop of traversing
     */
    public static void traverseTree(AbstractPart part, Consumer<AbstractPart> visitor,
                                    Predicate<AbstractPart> stopCondition) {
        traverseTree(part, visitor, Integer.MAX_VALUE, stopCondition);
    }

    /**
     * Traverses the AbstractPart nodes tree and applies the visitor function to each node with limited depth.
     * Uses a pre-order traversal (root-left-right).
     * The visitor function can be used to modify the node's state or to extract information.
     *
     * @param part    The current node being visited (start with the root)
     * @param visitor The function to apply to each AbstractPart node
     * @param depth   The depth to limit traversing at
     */
    public static void traverseTree(AbstractPart part, Consumer<AbstractPart> visitor, int depth,
                                    Predicate<AbstractPart> stopCondition) {
        if (part == null) return;

        // Stop if the depth limit is reached
        if (depth < 0) {
            return;
        }

        // Stop traversing this branch if the stop condition is met
        if (stopCondition != null && stopCondition.test(part)) {
            return;
        }

        visitor.accept(part);

        if (part.getPartType() == EXPRESSION_CONTAINER && depth > 0) {
            ExpressionContainer container = (ExpressionContainer) part;
            traverseTree(container.getLeft(), visitor, depth - 1, stopCondition);
            traverseTree(container.getRight(), visitor, depth - 1, stopCondition);
        }

        if (part.getPartType() == AbstractPart.PartType.AND_STRUCTURE && depth > 0) {
            List<AbstractPart> containerList = ((AndStructure) part).getOperands();
            containerList.forEach(container -> traverseTree(container, visitor, depth - 1, stopCondition));
        }

        if (part.getPartType() == AbstractPart.PartType.FUNCTION_ARGS && depth > 0) {
            List<AbstractPart> operands = ((FunctionArgs) part).getOperands();
            operands.forEach(operand -> traverseTree(operand, visitor, depth - 1, stopCondition));
        }
    }
}
