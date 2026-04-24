package com.aerospike.ael.visitor;

import com.aerospike.ael.ConditionBaseVisitor;
import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.fluent.AerospikeComparator;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer;
import com.aerospike.ael.parts.cdt.list.ListIndex;
import com.aerospike.ael.parts.cdt.list.ListIndexRange;
import com.aerospike.ael.parts.cdt.list.ListRank;
import com.aerospike.ael.parts.cdt.list.ListRankRange;
import com.aerospike.ael.parts.cdt.list.ListRankRangeRelative;
import com.aerospike.ael.parts.cdt.list.ListTypeDesignator;
import com.aerospike.ael.parts.cdt.list.ListValue;
import com.aerospike.ael.parts.cdt.list.ListValueList;
import com.aerospike.ael.parts.cdt.list.ListValueRange;
import com.aerospike.ael.parts.cdt.map.*;
import com.aerospike.ael.parts.controlstructure.AndStructure;
import com.aerospike.ael.parts.controlstructure.ExclusiveStructure;
import com.aerospike.ael.parts.controlstructure.LetStructure;
import com.aerospike.ael.parts.controlstructure.OrStructure;
import com.aerospike.ael.parts.controlstructure.WhenStructure;
import com.aerospike.ael.parts.operand.*;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.parts.path.BinPart;
import com.aerospike.ael.parts.path.Path;
import com.aerospike.ael.parts.path.PathFunction;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static com.aerospike.ael.util.ParsingUtils.*;
import static com.aerospike.ael.visitor.VisitorUtils.*;

public class ExpressionConditionVisitor extends ConditionBaseVisitor<AbstractPart> {

    private int letNestingDepth = 0;

    private static final Pattern VARIABLE_NAME_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern FUNCTION_NAME_PATTERN = Pattern.compile("[a-zA-Z][a-zA-Z0-9]*");

    @Override
    public AbstractPart visitLetExpression(ConditionParser.LetExpressionContext ctx) {
        letNestingDepth++;
        try {
            List<LetOperand> expressions = new ArrayList<>();

            for (ConditionParser.VariableDefinitionContext vdc : ctx.variableDefinition()) {
                String varName = vdc.NAME_IDENTIFIER().getText();
                validateVariableName(varName);
                AbstractPart part = visit(vdc.expression());
                LetOperand letOperand = new LetOperand(part, varName);
                expressions.add(letOperand);
            }
            // last expression is the action (described after "then")
            expressions.add(new LetOperand(visit(ctx.expression()), true));
            if (letNestingDepth == 1) {
                validateInVariableBindings(expressions);
            }
            return new ExpressionContainer(new LetStructure(expressions),
                    ExpressionContainer.ExprPartsOperation.LET_STRUCTURE);
        } finally {
            letNestingDepth--;
        }
    }

    @Override
    public AbstractPart visitWhenExpression(ConditionParser.WhenExpressionContext ctx) {
        List<AbstractPart> parts = new ArrayList<>();
        // iterate through each definition declaration
        for (ConditionParser.ExpressionMappingContext emc : ctx.expressionMapping()) {
            // visit condition
            parts.add(visit(emc.expression(0)));
            // visit action
            parts.add(visit(emc.expression(1)));
        }
        // visit default
        parts.add(visit(ctx.expression()));
        return new ExpressionContainer(new WhenStructure(parts), ExpressionContainer.ExprPartsOperation.WHEN_STRUCTURE);
    }

    @Override
    public AbstractPart visitAndExpression(ConditionParser.AndExpressionContext ctx) {
        if (ctx.comparisonExpression().size() == 1) {
            return visit(ctx.comparisonExpression(0));
        }

        List<ExpressionContainer> expressions = new ArrayList<>();
        for (ConditionParser.ComparisonExpressionContext ec : ctx.comparisonExpression()) {
            ExpressionContainer expr = (ExpressionContainer) visit(ec);
            if (expr == null) return null;

            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr);
        }
        return new ExpressionContainer(new AndStructure(expressions), ExpressionContainer.ExprPartsOperation.AND_STRUCTURE);
    }

    @Override
    public AbstractPart visitOrExpression(ConditionParser.OrExpressionContext ctx) {
        // If there's only one andExpression and no 'or' operators, just pass through
        if (ctx.logicalAndExpression().size() == 1) {
            return visit(ctx.logicalAndExpression(0));
        }

        List<ExpressionContainer> expressions = new ArrayList<>();
        // iterate through each sub-expression
        for (ConditionParser.LogicalAndExpressionContext ec : ctx.logicalAndExpression()) {
            ExpressionContainer expr = (ExpressionContainer) visit(ec);
            if (expr == null) return null;

            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr);
        }
        return new ExpressionContainer(new OrStructure(expressions), ExpressionContainer.ExprPartsOperation.OR_STRUCTURE);
    }

    @Override
    public AbstractPart visitNotExpression(ConditionParser.NotExpressionContext ctx) {
        ExpressionContainer expr = (ExpressionContainer) visit(ctx.expression());

        logicalSetBinAsBooleanExpr(expr);
        return new ExpressionContainer(expr, ExpressionContainer.ExprPartsOperation.NOT);
    }

    @Override
    public AbstractPart visitExclusiveExpression(ConditionParser.ExclusiveExpressionContext ctx) {
        if (ctx.expression().size() < 2) {
            throw new AelParseException("Exclusive logical operator requires 2 or more expressions");
        }
        List<ExpressionContainer> expressions = new ArrayList<>();
        // iterate through each sub-expression
        for (ConditionParser.ExpressionContext ec : ctx.expression()) {
            ExpressionContainer expr = (ExpressionContainer) visit(ec);
            logicalSetBinAsBooleanExpr(expr);
            expressions.add(expr);
        }
        return new ExpressionContainer(new ExclusiveStructure(expressions),
                ExpressionContainer.ExprPartsOperation.EXCLUSIVE_STRUCTURE);
    }

    @Override
    public AbstractPart visitBitwiseExpressionWrapper(ConditionParser.BitwiseExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.bitwiseExpression());
    }

    @Override
    public AbstractPart visitMultiplicativeExpressionWrapper(ConditionParser.MultiplicativeExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.multiplicativeExpression());
    }

    @Override
    public AbstractPart visitUnaryExpressionWrapper(ConditionParser.UnaryExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.unaryExpression());
    }

    @Override
    public AbstractPart visitShiftExpressionWrapper(ConditionParser.ShiftExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.shiftExpression());
    }

    @Override
    public AbstractPart visitAdditiveExpressionWrapper(ConditionParser.AdditiveExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.additiveExpression());
    }

    @Override
    public AbstractPart visitUnaryPlusExpression(ConditionParser.UnaryPlusExpressionContext ctx) {
        // Unary '+' is a no-op, delegate to the inner expression
        return visit(ctx.unaryExpression());
    }

    @Override
    public AbstractPart visitUnaryMinusExpression(ConditionParser.UnaryMinusExpressionContext ctx) {
        // Pre-inspect the parse tree (no visitor dispatch) to detect the Long.MIN_VALUE literal
        // (9223372036854775808), which cannot be parsed by visitIntOperand as a positive long.
        // If detected, we construct IntOperand(Long.MIN_VALUE) directly without visiting.
        AbstractPart longMinLiteral = tryParseLongMinLiteral(ctx.unaryExpression());
        if (longMinLiteral != null) {
            return longMinLiteral;
        }

        AbstractPart operand = visit(ctx.unaryExpression());

        // Negate literal operands directly
        if (operand instanceof IntOperand intOp) {
            return new IntOperand(negateLongLiteral(intOp.getValue(), ctx.getText()));
        }
        if (operand instanceof FloatOperand floatOp) {
            return new FloatOperand(-floatOp.getValue());
        }

        // For type-cast expressions (TO_INT, TO_FLOAT) with a literal operand,
        // push negation into the literal to preserve Exp tree equivalence (e.g., -5.asFloat())
        if (operand instanceof ExpressionContainer container && container.isUnary()) {
            ExpressionContainer.ExprPartsOperation op = container.getOperationType();
            if (op == ExpressionContainer.ExprPartsOperation.TO_INT
                    || op == ExpressionContainer.ExprPartsOperation.TO_FLOAT) {
                AbstractPart inner = container.getLeft();
                if (inner instanceof IntOperand intOp) {
                    return new ExpressionContainer(new IntOperand(negateLongLiteral(intOp.getValue(), ctx.getText())), op);
                }
                if (inner instanceof FloatOperand floatOp) {
                    return new ExpressionContainer(new FloatOperand(-floatOp.getValue()), op);
                }
            }
        }

        // General case: negate via 0 - operand
        return new ExpressionContainer(new IntOperand(0L), operand, ExpressionContainer.ExprPartsOperation.SUB);
    }

    private AbstractPart tryParseLongMinLiteral(ConditionParser.UnaryExpressionContext unaryCtx) {
        if (!(unaryCtx instanceof ConditionParser.OperandExpressionContext operandExpressionCtx)) {
            return null;
        }
        ConditionParser.OperandContext operandCtx = operandExpressionCtx.operand();

        ConditionParser.NumberOperandContext numberOperandCtx = null;
        ConditionParser.PathFunctionCastContext castCtx = null;
        if (operandCtx.numberOperand() != null) {
            numberOperandCtx = operandCtx.numberOperand();
        } else if (operandCtx.operandCast() != null) {
            numberOperandCtx = operandCtx.operandCast().numberOperand();
            castCtx = operandCtx.operandCast().pathFunctionCast();
        }

        if (numberOperandCtx == null || numberOperandCtx.intOperand() == null) {
            return null;
        }

        long parsed = parseUnsignedLongLiteral(numberOperandCtx.intOperand().INT().getText());
        if (parsed != Long.MIN_VALUE) {
            return null;
        }

        if (castCtx == null) {
            return new IntOperand(Long.MIN_VALUE);
        }

        String castText = castCtx.PATH_FUNCTION_CAST().getText();
        String typeVal = extractTypeFromMethod(castText);
        PathFunction.CastType castType = PathFunction.CastType.valueOf(typeVal.toUpperCase());
        ExpressionContainer.ExprPartsOperation op = switch (castType) {
            case INT -> ExpressionContainer.ExprPartsOperation.TO_INT;
            case FLOAT -> ExpressionContainer.ExprPartsOperation.TO_FLOAT;
        };
        return new ExpressionContainer(new IntOperand(Long.MIN_VALUE), op);
    }

    private static long negateLongLiteral(long value, String input) {
        if (value == Long.MIN_VALUE) {
            throw new AelParseException("Integer literal out of range: " + input);
        }
        return -value;
    }

    @Override
    public AbstractPart visitInExpression(ConditionParser.InExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        validateInRightOperand(left, right);
        inferInTypes(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.IN);
    }

    /**
     * Validates that both operands of an IN expression are non-null and that
     * the right operand is a list-compatible type (LIST_OPERAND, BIN_PART,
     * PATH_OPERAND, VARIABLE_OPERAND, or PLACEHOLDER_OPERAND).
     *
     * @throws AelParseException if either operand is null or the right operand
     *                           is not a list-compatible type
     */
    private static void validateInRightOperand(AbstractPart left, AbstractPart right) {
        if (left == null) {
            throw new AelParseException("Unable to parse left operand");
        }
        if (right == null) {
            throw new AelParseException("Unable to parse right operand");
        }
        if (right.getPartType() == AbstractPart.PartType.PLACEHOLDER_OPERAND
                || right.getPartType() == AbstractPart.PartType.LIST_OPERAND
                || right.getPartType() == AbstractPart.PartType.BIN_PART
                || right.getPartType() == AbstractPart.PartType.PATH_OPERAND
                || right.getPartType() == AbstractPart.PartType.VARIABLE_OPERAND) {
            return;
        }
        throw new AelParseException("IN operation requires a List as the right operand");
    }

    /**
     * Infers and validates Exp types for IN expression operands.
     * <p>
     * For the right operand: if it is a BIN_PART without an explicit type, its
     * type is set to LIST; if it has an explicit non-LIST type, an error is thrown.
     * If it is a PATH_OPERAND with a path function whose type is non-LIST, an error
     * is thrown.
     * <p>
     * For the left operand: validates that the type is not ambiguous, then delegates
     * to {@link VisitorUtils#validateListHomogeneity}
     * and {@link VisitorUtils#inferBinTypeFromList} to enforce uniform list element types
     * and infer the bin type when the right operand is a list literal.
     */
    private static void inferInTypes(AbstractPart left, AbstractPart right) {
        if (right.getPartType() == AbstractPart.PartType.BIN_PART) {
            BinPart rightBin = (BinPart) right;
            if (!rightBin.isTypeExplicitlySet()) {
                rightBin.updateExp(Exp.Type.LIST);
            } else if (rightBin.getExpType() != Exp.Type.LIST) {
                throw new AelParseException(
                        "IN operation requires a List as the right operand");
            }
        } else if (right.getPartType() == AbstractPart.PartType.PATH_OPERAND) {
            if (isPathExplicitlyNonList((Path) right)) {
                throw new AelParseException(
                        "IN operation requires a List as the right operand");
            }
        }
        Exp.Type inferredType = VisitorUtils.validateListHomogeneity(right);
        validateLeftTypeNotAmbiguous(left, inferredType);
        VisitorUtils.inferBinTypeFromList(left, inferredType);
    }

    /**
     * Validates that the left operand of an IN expression has a deterministic type.
     * <p>
     * When the right operand is a typed list literal, the left type can be inferred
     * from the list elements ({@code inferredType != null}). Otherwise, BIN_PART
     * and PATH_OPERAND operands must carry an explicit type annotation
     * (e.g. {@code .get(type: INT)}, {@code .asInt()}, {@code []}, {@code {}}).
     *
     * @param left         the left operand of the IN expression
     * @param inferredType the element type inferred from the right list (may be {@code null})
     * @throws AelParseException if the left operand type cannot be determined
     */
    private static void validateLeftTypeNotAmbiguous(AbstractPart left, Exp.Type inferredType) {
        if (inferredType != null) {
            return;
        }
        if (!isLeftTypeAmbiguous(left)) {
            return;
        }
        throw new AelParseException(
                "cannot infer the type of the left operand for IN operation; "
                        + "use .get(type: <TYPE>) to specify it");
    }

    /**
     * Checks whether a left operand of IN has ambiguous type.
     * <p>
     * Only {@code BIN_PART} and {@code PATH_OPERAND} can be ambiguous.
     * Literals, expressions, placeholders, and variables always carry
     * concrete types and are never ambiguous.
     */
    private static boolean isLeftTypeAmbiguous(AbstractPart left) {
        if (left.getPartType() == AbstractPart.PartType.BIN_PART) {
            return !((BinPart) left).isTypeExplicitlySet();
        }
        if (left.getPartType() == AbstractPart.PartType.PATH_OPERAND) {
            return isPathTypeAmbiguous((Path) left);
        }
        return false;
    }

    private enum PathListClassification { DEFINITELY_LIST, DEFINITELY_NOT_LIST, UNKNOWN }

    /**
     * Classifies a PATH_OPERAND as definitely a list, definitely not a list, or unknown.
     * <p>
     * Classification is based on (checked in order):
     * <ul>
     *   <li>a path function with an explicit binType — LIST vs non-LIST,</li>
     *   <li>a COUNT or SIZE path function — always INT (DEFINITELY_NOT_LIST),</li>
     *   <li>a list type designator ({@code []}) as the last CDT part — DEFINITELY_LIST,</li>
     *   <li>a map type designator ({@code {}}) as the last CDT part — DEFINITELY_NOT_LIST.</li>
     * </ul>
     * Falls back to UNKNOWN when none of the above applies.
     */
    private static PathListClassification classifyPathListness(Path path) {
        PathFunction pathFunc = path.getPathFunction();
        if (pathFunc != null) {
            // CAST is also covered here: always constructed with a non-null binType
            if (pathFunc.getBinType() != null) {
                return pathFunc.getBinType() == Exp.Type.LIST
                        ? PathListClassification.DEFINITELY_LIST
                        : PathListClassification.DEFINITELY_NOT_LIST;
            }
            PathFunction.PathFunctionType pathFuncType = pathFunc.getPathFunctionType();
            if (pathFuncType == PathFunction.PathFunctionType.COUNT
                    || pathFuncType == PathFunction.PathFunctionType.SIZE) {
                return PathListClassification.DEFINITELY_NOT_LIST;
            }
        }
        List<AbstractPart> cdtParts = path.getBasePath().getCdtParts();
        if (!cdtParts.isEmpty()) {
            AbstractPart lastCdt = cdtParts.get(cdtParts.size() - 1);
            if (lastCdt instanceof ListTypeDesignator) {
                return PathListClassification.DEFINITELY_LIST;
            }
            if (lastCdt instanceof MapTypeDesignator) {
                return PathListClassification.DEFINITELY_NOT_LIST;
            }
        }
        return PathListClassification.UNKNOWN;
    }

    private static boolean isPathTypeAmbiguous(Path path) {
        return classifyPathListness(path) == PathListClassification.UNKNOWN;
    }

    private static boolean isPathExplicitlyNonList(Path path) {
        return classifyPathListness(path) == PathListClassification.DEFINITELY_NOT_LIST;
    }

    /**
     * Validates that variables used as the right operand of an IN expression
     * within a LET block are bound to list-compatible values.
     * <p>
     * Variable definitions are known at parse time, so scalar/map bindings
     * can be rejected early instead of deferring to server-side failure.
     */
    private static void validateInVariableBindings(List<LetOperand> expressions) {
        // LET block always has at least one variable definition (grammar-enforced)
        Map<String, AbstractPart> varBindings = new HashMap<>();
        for (LetOperand operand : expressions) {
            if (!operand.isLastPart()) {
                validateInVariablesInTree(operand.getPart(), varBindings);
                varBindings.put(operand.getString(), operand.getPart());
            }
        }
        validateInVariablesInTree(getLetBody(expressions), varBindings);
    }

    private static AbstractPart getLetBody(List<LetOperand> operands) {
        return operands.get(operands.size() - 1).getPart();
    }

    // Handles every AbstractPart subclass that can contain expression children:
    // ExpressionContainer, LetStructure, And/Or/ExclusiveStructure, WhenStructure, FunctionArgs.
    // Leaf types (operands, BinPart, Path, etc.) are terminal — no recursion needed.
    // When adding a new composite AbstractPart subclass, add a branch here.
    private static void validateInVariablesInTree(AbstractPart part,
                                                  Map<String, AbstractPart> varBindings) {
        if (part instanceof ExpressionContainer expr) {
            validateInVariableIsListCompatible(expr, varBindings);
            if (expr.getLeft() != null) {
                validateInVariablesInTree(expr.getLeft(), varBindings);
            }
            if (!expr.isUnary() && expr.getRight() != null) {
                validateInVariablesInTree(expr.getRight(), varBindings);
            }
        } else if (part instanceof LetStructure ws) {
            Map<String, AbstractPart> merged = new HashMap<>(varBindings);
            for (LetOperand op : ws.getOperands()) {
                if (!op.isLastPart()) {
                    validateInVariablesInTree(op.getPart(), merged);
                    merged.put(op.getString(), op.getPart());
                }
            }
            validateInVariablesInTree(getLetBody(ws.getOperands()), merged);
        } else if (part instanceof AndStructure s) {
            s.getOperands().forEach(op -> validateInVariablesInTree(op, varBindings));
        } else if (part instanceof OrStructure s) {
            s.getOperands().forEach(op -> validateInVariablesInTree(op, varBindings));
        } else if (part instanceof ExclusiveStructure s) {
            s.getOperands().forEach(op -> validateInVariablesInTree(op, varBindings));
        } else if (part instanceof WhenStructure s) {
            s.getOperands().forEach(op -> validateInVariablesInTree(op, varBindings));
        } else if (part instanceof FunctionArgs fa) {
            fa.getOperands().forEach(op -> validateInVariablesInTree(op, varBindings));
        }
    }

    private static void validateInVariableIsListCompatible(ExpressionContainer expr,
                                                          Map<String, AbstractPart> varBindings) {
        if (expr.getOperationType() != ExpressionContainer.ExprPartsOperation.IN) return;
        if (expr.getRight() == null) return;
        if (expr.getRight().getPartType() != AbstractPart.PartType.VARIABLE_OPERAND) return;

        String varName = ((VariableOperand) expr.getRight()).getValue();
        AbstractPart boundPart = varBindings.get(varName);
        if (boundPart != null && isNotList(boundPart)) {
            throw new AelParseException(
                    "IN operation requires a List as the right operand; "
                            + "variable '" + varName + "' contains a non-List type");
        }
    }

    private static final EnumSet<AbstractPart.PartType> NOT_LIST_TYPES = EnumSet.of(
            AbstractPart.PartType.INT_OPERAND,
            AbstractPart.PartType.FLOAT_OPERAND,
            AbstractPart.PartType.BOOL_OPERAND,
            AbstractPart.PartType.STRING_OPERAND,
            AbstractPart.PartType.MAP_OPERAND,
            AbstractPart.PartType.METADATA_OPERAND,
            AbstractPart.PartType.BLOB_OPERAND
    );

    private static boolean isNotList(AbstractPart part) {
        if (NOT_LIST_TYPES.contains(part.getPartType())) {
            return true;
        }
        if (part instanceof BinPart bin) {
            return bin.isTypeExplicitlySet() && bin.getExpType() != Exp.Type.LIST;
        }
        if (part.getPartType() == AbstractPart.PartType.PATH_OPERAND) {
            return isPathExplicitlyNonList((Path) part);
        }
        if (part instanceof ExpressionContainer expr && expr.getOperationType() != null) {
            return expr.getOperationType().isScalar();
        }
        return false;
    }

    @Override
    public AbstractPart visitGreaterThanExpression(ConditionParser.GreaterThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.GT);
    }

    @Override
    public AbstractPart visitGreaterThanOrEqualExpression(ConditionParser.GreaterThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.GTEQ);
    }

    @Override
    public AbstractPart visitLessThanExpression(ConditionParser.LessThanExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.LT);
    }

    @Override
    public AbstractPart visitLessThanOrEqualExpression(ConditionParser.LessThanOrEqualExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.LTEQ);
    }

    @Override
    public AbstractPart visitEqualityExpression(ConditionParser.EqualityExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.EQ);
    }

    @Override
    public AbstractPart visitInequalityExpression(ConditionParser.InequalityExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression(0));
        AbstractPart right = visit(ctx.bitwiseExpression(1));

        overrideTypeInfo(left, right);

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.NOTEQ);
    }

    @Override
    public AbstractPart visitAddExpression(ConditionParser.AddExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression());
        AbstractPart right = visit(ctx.multiplicativeExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.ADD);
    }

    @Override
    public AbstractPart visitSubExpression(ConditionParser.SubExpressionContext ctx) {
        AbstractPart left = visit(ctx.additiveExpression());
        AbstractPart right = visit(ctx.multiplicativeExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.SUB);
    }

    @Override
    public AbstractPart visitMulExpression(ConditionParser.MulExpressionContext ctx) {
        AbstractPart left = visit(ctx.multiplicativeExpression());
        AbstractPart right = visit(ctx.powerExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.MUL);
    }

    @Override
    public AbstractPart visitDivExpression(ConditionParser.DivExpressionContext ctx) {
        AbstractPart left = visit(ctx.multiplicativeExpression());
        AbstractPart right = visit(ctx.powerExpression());

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.DIV);
    }

    @Override
    public AbstractPart visitModExpression(ConditionParser.ModExpressionContext ctx) {
        AbstractPart left = visit(ctx.multiplicativeExpression()); // first operand
        AbstractPart right = visit(ctx.powerExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.MOD);
    }

    @Override
    public AbstractPart visitPowerExpressionWrapper(ConditionParser.PowerExpressionWrapperContext ctx) {
        // Pass through the wrapper
        return visit(ctx.powerExpression());
    }

    @Override
    public AbstractPart visitPowExpression(ConditionParser.PowExpressionContext ctx) {
        AbstractPart left = visit(ctx.powerExpression(0));
        AbstractPart right = visit(ctx.powerExpression(1));

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.POW);
    }

    @Override
    public AbstractPart visitFunctionCall(ConditionParser.FunctionCallContext ctx) {
        // If error recovery created this node without actual parentheses, fail fast
        if (ctx.getChildCount() < 3 || !ctx.getChild(1).getText().equals("(")) {
            String name = ctx.getChildCount() > 0 ? ctx.getChild(0).getText() : "<unknown>";
            throw new AelParseException("Unexpected identifier: " + name);
        }

        String funcName = ctx.NAME_IDENTIFIER().getText();
        validateFunctionName(funcName);
        List<AbstractPart> args = new ArrayList<>();
        for (ConditionParser.ExpressionContext ec : ctx.expression()) {
            args.add(visit(ec));
        }

        return switch (funcName) {
            // Unary arithmetic functions
            case "abs" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0), ExpressionContainer.ExprPartsOperation.ABS);
            }
            case "ceil" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0), ExpressionContainer.ExprPartsOperation.CEIL);
            }
            case "floor" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0), ExpressionContainer.ExprPartsOperation.FLOOR);
            }
            // Binary arithmetic functions
            case "log" -> {
                validateFunctionArgCount(funcName, args, 2);
                yield new ExpressionContainer(args.get(0), args.get(1),
                        ExpressionContainer.ExprPartsOperation.LOG);
            }
            // Variadic arithmetic functions
            case "min" -> {
                validateFunctionArgCountAtLeast(funcName, args, 2);
                yield new ExpressionContainer(new FunctionArgs(args),
                        ExpressionContainer.ExprPartsOperation.MIN_FUNC);
            }
            case "max" -> {
                validateFunctionArgCountAtLeast(funcName, args, 2);
                yield new ExpressionContainer(new FunctionArgs(args),
                        ExpressionContainer.ExprPartsOperation.MAX_FUNC);
            }
            // Bit-scanning functions
            case "countOneBits" -> {
                validateFunctionArgCount(funcName, args, 1);
                yield new ExpressionContainer(args.get(0),
                        ExpressionContainer.ExprPartsOperation.COUNT_ONE_BITS);
            }
            case "findBitLeft" -> {
                validateFunctionArgCount(funcName, args, 2);
                yield new ExpressionContainer(args.get(0), args.get(1),
                        ExpressionContainer.ExprPartsOperation.FIND_BIT_LEFT);
            }
            case "findBitRight" -> {
                validateFunctionArgCount(funcName, args, 2);
                yield new ExpressionContainer(args.get(0), args.get(1),
                        ExpressionContainer.ExprPartsOperation.FIND_BIT_RIGHT);
            }
            default -> throw new AelParseException("Unknown function: " + funcName);
        };
    }

    private void validateFunctionArgCount(String funcName, List<AbstractPart> args, int expected) {
        if (args.size() != expected) {
            throw new AelParseException(
                    "Function '%s' expects %d argument(s), got %d".formatted(funcName, expected, args.size()));
        }
    }

    private void validateFunctionArgCountAtLeast(String funcName, List<AbstractPart> args, int minCount) {
        if (args.size() < minCount) {
            throw new AelParseException(
                    "Function '%s' expects at least %d arguments, got %d".formatted(funcName, minCount, args.size()));
        }
    }

    @Override
    public AbstractPart visitIntAndExpression(ConditionParser.IntAndExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression()); // first operand
        AbstractPart right = visit(ctx.shiftExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.INT_AND);
    }

    @Override
    public AbstractPart visitIntOrExpression(ConditionParser.IntOrExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression()); // first operand
        AbstractPart right = visit(ctx.shiftExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.INT_OR);
    }

    @Override
    public AbstractPart visitIntXorExpression(ConditionParser.IntXorExpressionContext ctx) {
        AbstractPart left = visit(ctx.bitwiseExpression()); // first operand
        AbstractPart right = visit(ctx.shiftExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.INT_XOR);
    }

    @Override
    public AbstractPart visitIntNotExpression(ConditionParser.IntNotExpressionContext ctx) {
        AbstractPart operand = visit(ctx.unaryExpression());

        return new ExpressionContainer(operand, ExpressionContainer.ExprPartsOperation.INT_NOT);
    }

    @Override
    public AbstractPart visitIntLShiftExpression(ConditionParser.IntLShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.shiftExpression()); // first operand
        AbstractPart right = visit(ctx.additiveExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.L_SHIFT);
    }

    @Override
    public AbstractPart visitIntRShiftExpression(ConditionParser.IntRShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.shiftExpression()); // first operand
        AbstractPart right = visit(ctx.additiveExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.R_SHIFT);
    }

    @Override
    public AbstractPart visitIntLogicalRShiftExpression(ConditionParser.IntLogicalRShiftExpressionContext ctx) {
        AbstractPart left = visit(ctx.shiftExpression()); // first operand
        AbstractPart right = visit(ctx.additiveExpression()); // second operand

        return new ExpressionContainer(left, right, ExpressionContainer.ExprPartsOperation.LOGICAL_R_SHIFT);
    }

    @Override
    public AbstractPart visitPathFunctionGet(ConditionParser.PathFunctionGetContext ctx) {
        PathFunction.ReturnParam returnParam = null;
        Exp.Type binType = null;
        if (ctx.pathFunctionParams() != null) {
            for (ConditionParser.PathFunctionParamContext paramCtx : ctx.pathFunctionParams().pathFunctionParam()) {
                if (paramCtx != null) {
                    String typeVal = getPathFunctionParam(paramCtx, "type");
                    if (typeVal != null) binType = Exp.Type.valueOf(typeVal);
                    String returnVal = getPathFunctionParam(paramCtx, "return");
                    if (returnVal != null) returnParam = PathFunction.ReturnParam.valueOf(returnVal);
                }
            }
        }
        return new PathFunction(PathFunction.PathFunctionType.GET, returnParam, binType);
    }

    @Override
    public AbstractPart visitPathFunctionCount(ConditionParser.PathFunctionCountContext ctx) {
        // todo: TYPE_PARAM?
        return new PathFunction(PathFunction.PathFunctionType.COUNT, PathFunction.ReturnParam.COUNT, null);
    }

    @Override
    public AbstractPart visitPathFunctionExists(ConditionParser.PathFunctionExistsContext ctx) {
        return new PathFunction(PathFunction.PathFunctionType.EXISTS, PathFunction.ReturnParam.EXISTS, null);
    }

    @Override
    public AbstractPart visitPathFunctionCast(ConditionParser.PathFunctionCastContext ctx) {
        String typeVal = extractTypeFromMethod(ctx.PATH_FUNCTION_CAST().getText());
        PathFunction.CastType castType = PathFunction.CastType.valueOf(typeVal.toUpperCase());
        Exp.Type binType = PathFunction.castTypeToExpType(castType);

        return new PathFunction(PathFunction.PathFunctionType.CAST, null, binType);
    }

    @Override
    public AbstractPart visitOperandCast(ConditionParser.OperandCastContext ctx) {
        AbstractPart numberOperand = visit(ctx.numberOperand());

        String castText = ctx.pathFunctionCast().PATH_FUNCTION_CAST().getText();
        String typeVal = extractTypeFromMethod(castText);
        PathFunction.CastType castType = PathFunction.CastType.valueOf(typeVal.toUpperCase());

        ExpressionContainer.ExprPartsOperation op = switch (castType) {
            case INT -> ExpressionContainer.ExprPartsOperation.TO_INT;
            case FLOAT -> ExpressionContainer.ExprPartsOperation.TO_FLOAT;
        };

        return new ExpressionContainer(numberOperand, op);
    }

    @Override
    public AbstractPart visitMetadata(ConditionParser.MetadataContext ctx) {
        String text = ctx.METADATA_FUNCTION().getText();
        String functionName = extractFunctionName(text);
        Integer parameter = extractParameter(text);

        if (parameter != null) {
            return new MetadataOperand(functionName, parameter);
        } else {
            return new MetadataOperand(functionName);
        }
    }

    @Override
    public AbstractPart visitBinPart(ConditionParser.BinPartContext ctx) {
        if (ctx.BIN_IDENTIFIER() != null) {
            String binName = ctx.BIN_IDENTIFIER().getText();
            rejectBinNameContainingNull(binName);
            return new BinPart(binName);
        }
        if (ctx.QUOTED_STRING() != null) {
            String quoted = ctx.QUOTED_STRING().getText();
            if (quoted.length() <= 2) {
                throw new AelParseException("Bin name must not be empty");
            }
            String binName = unquote(quoted);
            rejectBinNameContainingNull(binName);
            return new BinPart(binName);
        }
        // Fallthrough: NAME_IDENTIFIER, IN, TRUE, FALSE, and all keyword literals ('and', 'or',
        // 'not', etc.). ctx.getText() returns the matched text preserving original case.
        String binName = ctx.getText();
        rejectBinNameContainingNull(binName);
        return new BinPart(binName);
    }

    private static void validateVariableName(String name) {
        if (!VARIABLE_NAME_PATTERN.matcher(name).matches()) {
            throw new AelParseException(
                    "Invalid variable name '%s': must start with a letter or underscore".formatted(name));
        }
    }

    private static void validateFunctionName(String name) {
        if (!FUNCTION_NAME_PATTERN.matcher(name).matches()) {
            throw new AelParseException(
                    "Invalid function name '%s': must start with a letter and contain only letters and digits".formatted(name));
        }
    }

    private static void rejectBinNameContainingNull(String binName) {
        if (binName.toLowerCase(Locale.ROOT).contains("null")) {
            throw new AelParseException(
                    "Bin name must not contain the reserved word 'null': " + binName);
        }
    }

    @Override
    public AbstractPart visitOperandExpression(ConditionParser.OperandExpressionContext ctx) {
        return visit(ctx.operand());
    }

    @Override
    public AbstractPart visitListConstant(ConditionParser.ListConstantContext ctx) {
        if (ctx.LIST_TYPE_DESIGNATOR() != null) {
            return new ListOperand(Collections.emptyList());
        }
        return readChildrenIntoListOperand(ctx);
    }

    public ListOperand readChildrenIntoListOperand(RuleNode listNode) {
        int size = listNode.getChildCount();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ParseTree child = listNode.getChild(i);
            if (!shouldVisitListElement(i, size, child)) {
                continue;
            }

            AbstractPart operand = visit(child); // delegate to a dedicated visitor
            if (operand == null) {
                throw new AelParseException("Unable to parse list operand");
            }

            try {
                values.add(((ParsedValueOperand) operand).getValue());
            } catch (ClassCastException e) {
                throw new AelParseException("List constant contains elements of different type");
            }
        }

        return new ListOperand(values);
    }

    @Override
    public AbstractPart visitOrderedMapConstant(ConditionParser.OrderedMapConstantContext ctx) {
        return readChildrenIntoMapOperand(ctx);
    }

    public SortedMap<Object, Object> getOrderedMapPair(ParseTree ctx) {
        if (ctx.getChild(0) == null || ctx.getChild(2) == null) {
            throw new AelParseException("Unable to parse map operand");
        }

        Object key = ((ParsedValueOperand) visit(ctx.getChild(0))).getValue();
        Object value = ((ParsedValueOperand) visit(ctx.getChild(2))).getValue();

        SortedMap<Object, Object> map = new TreeMap<>(new AerospikeComparator());
        map.put(key, value);

        return map;
    }

    public MapOperand readChildrenIntoMapOperand(RuleNode mapNode) {
        int size = mapNode.getChildCount();
        SortedMap<Object, Object> map = new TreeMap<>(new AerospikeComparator());
        for (int i = 0; i < size; i++) {
            ParseTree child = mapNode.getChild(i);
            if (!shouldVisitMapElement(i, size, child)) {
                continue;
            }

            SortedMap<Object, Object> mapOfPair = getOrderedMapPair(child); // delegate to a dedicated visitor

            try {
                mapOfPair.forEach(map::putIfAbsent); // put contents of the current map pair to the resulting map
            } catch (ClassCastException e) {
                throw new AelParseException("Map constant contains elements of different type");
            }
        }

        return new MapOperand(map);
    }

    @Override
    public AbstractPart visitStringOperand(ConditionParser.StringOperandContext ctx) {
        String text = unquote(ctx.getText());
        return new StringOperand(text);
    }

    @Override
    public AbstractPart visitBlobOperand(ConditionParser.BlobOperandContext ctx) {
        if (ctx.BLOB_LITERAL() != null) {
            return new BlobOperand(parseHexToBytes(ctx.BLOB_LITERAL().getText()));
        }
        return new BlobOperand(parseB64ToBytes(ctx.B64_LITERAL().getText()));
    }

    @Override
    public AbstractPart visitNumberOperand(ConditionParser.NumberOperandContext ctx) {
        // Delegates to specific visit methods
        return visitChildren(ctx);
    }

    @Override
    public AbstractPart visitIntOperand(ConditionParser.IntOperandContext ctx) {
        String text = ctx.INT().getText();
        return new IntOperand(parseIntLiteral(text));
    }

    /**
     * Parses an unsigned INT literal which may be decimal, hexadecimal (0x/0X), or binary (0b/0B).
     * Signs are handled at the parser level by the unaryExpression rule.
     */
    private static long parseIntLiteral(String text) {
        long parsed = parseUnsignedLongLiteral(text);
        if (parsed == Long.MIN_VALUE) {
            throw new AelParseException("Integer literal out of range: " + text);
        }
        return parsed;
    }

    /**
     * Parses unsigned FLOAT literals and leading-dot floats (e.g., ".37").
     * Signs are handled at the parser level by the unaryExpression rule.
     */
    @Override
    public AbstractPart visitFloatOperand(ConditionParser.FloatOperandContext ctx) {
        try {
            if (ctx.FLOAT() != null) {
                return new FloatOperand(Double.parseDouble(ctx.FLOAT().getText()));
            }
            String leadingDotFloat = ctx.LEADING_DOT_FLOAT().getText();
            return new FloatOperand(Double.parseDouble("0" + leadingDotFloat));
        } catch (NumberFormatException e) {
            throw new AelParseException("Invalid float literal: " + ctx.getText(), e);
        }
    }

    @Override
    public AbstractPart visitBooleanOperand(ConditionParser.BooleanOperandContext ctx) {
        String text = ctx.getText();
        return new BooleanOperand(Boolean.parseBoolean(text));
    }

    @Override
    public AbstractPart visitPlaceholder(ConditionParser.PlaceholderContext ctx) {
        // Extract index from the placeholder
        String placeholderText = ctx.getText();
        int index = Integer.parseInt(placeholderText.substring(1));
        return new PlaceholderOperand(index);
    }

    @Override
    public AbstractPart visitBasePath(ConditionParser.BasePathContext ctx) {
        BinPart binPart = null;
        List<AbstractPart> cdtParts = new ArrayList<>();
        // Filter out standalone '.' separator tokens. Embedded-dot tokens (pathIntMapKey,
        // pathHexBinaryMapKey) have multi-char text (e.g. ".55", ".0xff") and pass through.
        List<ParseTree> ctxChildrenExclDots = ctx.children.stream()
                .filter(tree -> !tree.getText().equals("."))
                .toList();

        for (ParseTree child : ctxChildrenExclDots) {
            AbstractPart part = visit(child);
            switch (part.getPartType()) {
                case BIN_PART -> binPart = (BinPart) part;
                case LIST_PART, MAP_PART -> cdtParts.add(part);
                default -> throw new AelParseException("Unexpected path part: %s".formatted(part.getPartType()));
            }
        }

        if (binPart == null) {
            throw new AelParseException("Expecting bin to be the first path part from the left");
        }

        return new BasePath(binPart, cdtParts);
    }

    @Override
    public AbstractPart visitVariable(ConditionParser.VariableContext ctx) {
        String text = ctx.VARIABLE_REFERENCE().getText();
        return new VariableOperand(extractVariableNameOrFail(text));
    }

    @Override
    public AbstractPart visitPath(ConditionParser.PathContext ctx) {
        BasePath basePath = (BasePath) visit(ctx.basePath());
        PathFunction pathFunction = visitPathFunctionIfPresent(ctx);

        if (pathFunction != null && pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.EXISTS) {
            if (basePath.getCdtParts().isEmpty()) {
                return buildBinExists(basePath.getBinPart());
            }
            return buildCdtExists(basePath, pathFunction);
        }

        if (!basePath.getCdtParts().isEmpty()
                || pathFunction != null && pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.COUNT) {
            return new Path(basePath, pathFunction);
        }

        if (pathFunction != null && pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.CAST) {
            return buildBinCast(basePath.getBinPart(), pathFunction);
        }

        overrideBinWithPathFunction(basePath.getBinPart(), pathFunction);
        return basePath.getBinPart();
    }

    private PathFunction visitPathFunctionIfPresent(ConditionParser.PathContext ctx) {
        return ctx.pathFunction() != null ? (PathFunction) visit(ctx.pathFunction()) : null;
    }

    private AbstractPart buildBinExists(BinPart binPart) {
        return new ExpressionContainer(binPart, ExpressionContainer.ExprPartsOperation.EXISTS);
    }

    private AbstractPart buildCdtExists(BasePath basePath, PathFunction pathFunction) {
        return new ExpressionContainer(
                new Path(basePath, pathFunction), ExpressionContainer.ExprPartsOperation.EXISTS);
    }

    private AbstractPart buildBinCast(BinPart binPart, PathFunction pathFunction) {
        String castText = pathFunction.getBinType().toString();
        PathFunction.CastType castType = PathFunction.CastType.valueOf(castText);
        binPart.updateExp(PathFunction.castSourceExpType(castType));
        binPart.setTypeExplicitlySet(true);
        return new ExpressionContainer(binPart, PathFunction.castTypeToOperation(castType));
    }

    private void overrideBinWithPathFunction(BinPart binPart, PathFunction pathFunction) {
        if (pathFunction != null) {
            Exp.Type type = pathFunction.getBinType();
            if (type != null) {
                binPart.updateExp(type);
                binPart.setTypeExplicitlySet(true);
            }
        }
    }

    @Override
    public AbstractPart visitListPart(ConditionParser.ListPartContext ctx) {
        if (ctx.LIST_TYPE_DESIGNATOR() != null) return ListTypeDesignator.from();
        if (ctx.listIndex() != null) return ListIndex.from(ctx.listIndex());
        if (ctx.listValue() != null) return ListValue.from(ctx.listValue());
        if (ctx.listRank() != null) return ListRank.from(ctx.listRank());
        if (ctx.listIndexRange() != null) return ListIndexRange.from(ctx.listIndexRange());
        if (ctx.listValueList() != null) return ListValueList.from(ctx.listValueList());
        if (ctx.listValueRange() != null) return ListValueRange.from(ctx.listValueRange());
        if (ctx.listRankRange() != null) return ListRankRange.from(ctx.listRankRange());
        if (ctx.listRankRangeRelative() != null)
            return ListRankRangeRelative.from(ctx.listRankRangeRelative());
        throw new AelParseException("Unexpected list part: %s".formatted(ctx.getText()));
    }

    @Override
    public AbstractPart visitPathIntMapKey(ConditionParser.PathIntMapKeyContext ctx) {
        String keyText = ctx.getText().substring(1); // strip leading dot
        return new MapKey(parseLongMapKey(keyText));
    }

    @Override
    public AbstractPart visitPathHexBinaryMapKey(ConditionParser.PathHexBinaryMapKeyContext ctx) {
        String tokenText = ctx.LEADING_DOT_FLOAT_HEX_OR_BINARY().getText();
        return new MapKey(parseUnsignedLongLiteral(tokenText.substring(1)));
    }

    @Override
    public AbstractPart visitMapPart(ConditionParser.MapPartContext ctx) {
        if (ctx.MAP_TYPE_DESIGNATOR() != null) return MapTypeDesignator.from();
        if (ctx.mapKey() != null) return MapKey.from(ctx.mapKey());
        if (ctx.mapIndex() != null) return MapIndex.from(ctx.mapIndex());
        if (ctx.mapValue() != null) return MapValue.from(ctx.mapValue());
        if (ctx.mapRank() != null) return MapRank.from(ctx.mapRank());
        if (ctx.mapKeyRange() != null) return MapKeyRange.from(ctx.mapKeyRange());
        if (ctx.mapKeyList() != null) return MapKeyList.from(ctx.mapKeyList());
        if (ctx.mapIndexRange() != null) return MapIndexRange.from(ctx.mapIndexRange());
        if (ctx.mapValueList() != null) return MapValueList.from(ctx.mapValueList());
        if (ctx.mapValueRange() != null) return MapValueRange.from(ctx.mapValueRange());
        if (ctx.mapRankRange() != null) return MapRankRange.from(ctx.mapRankRange());
        if (ctx.mapRankRangeRelative() != null)
            return MapRankRangeRelative.from(ctx.mapRankRangeRelative());
        if (ctx.mapIndexRangeRelative() != null)
            return MapIndexRangeRelative.from(ctx.mapIndexRangeRelative());
        throw new AelParseException("Unexpected map part: %s".formatted(ctx.getText()));
    }

    @Override
    protected AbstractPart aggregateResult(AbstractPart aggregate, AbstractPart nextResult) {
        return nextResult == null ? aggregate : nextResult;
    }
}
