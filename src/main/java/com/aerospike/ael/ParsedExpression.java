package com.aerospike.ael;

import com.aerospike.ael.annotation.Beta;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.query.Filter;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;
import java.util.Map;

import static com.aerospike.ael.parts.AbstractPart.PartType.EXPRESSION_CONTAINER;
import static com.aerospike.ael.visitor.VisitorUtils.buildExpr;

/**
 * A class to build and store the results of AEL expression parsing: parsed {@code expressionTree}, {@code indexesMap}
 * of given indexes, {@code placeholderValues} to match with placeholders and {@link ParseResult} that holds
 * a potential secondary index {@link Filter} and a potential {@link Exp}.
 */
@Beta
@Getter
public class ParsedExpression {

    private final AbstractPart expressionTree;
    private final Map<String, List<Index>> indexesMap;
    private final String preferredBin;
    private final PlaceholderValues placeholderValues;
    private ParseResult result;

    public ParsedExpression(AbstractPart exprTree, PlaceholderValues placeholderValues,
                            Map<String, List<Index>> indexesMap) {
        this(exprTree, placeholderValues, indexesMap, null);
    }

    public ParsedExpression(AbstractPart exprTree, PlaceholderValues placeholderValues,
                            Map<String, List<Index>> indexesMap, String preferredBin) {
        this.expressionTree = exprTree;
        this.placeholderValues = placeholderValues;
        this.indexesMap = indexesMap;
        this.preferredBin = preferredBin;
    }

    /**
     * @return {@link ParseResult} containing secondary index {@link Filter} and/or filter {@link Exp}.
     * Each can be null in case of invalid or unsupported AEL string
     * @throws AelParseException If there was an error
     */
    public ParseResult getResult() {
        if (result == null) {
            result = getResult(placeholderValues);
        }
        return result;
    }

    /**
     * Traverse expression tree using the given placeholder values
     *
     * @param placeholderValues {@link PlaceholderValues} to match with placeholders by index
     * @return {@link ParseResult} containing secondary index {@link Filter} and/or {@link Exp}.
     * Each can be null in case of invalid or unsupported AEL string
     * @throws AelParseException If there was an error
     */
    public ParseResult getResult(PlaceholderValues placeholderValues) {
        if (expressionTree != null) {
            if (expressionTree.getPartType() == EXPRESSION_CONTAINER) {
                AbstractPart resultPart = buildExpr(
                        (ExpressionContainer) expressionTree, placeholderValues, indexesMap, preferredBin);
                return new ParseResult(resultPart.getFilter(), resultPart.getExp());
            } else {
                return new ParseResult(expressionTree.getFilter(), expressionTree.getExp());
            }
        }
        return new ParseResult(null, null);
    }
}
