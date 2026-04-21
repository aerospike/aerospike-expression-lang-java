package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.parts.path.BasePath;

import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;
import static com.aerospike.ael.util.ParsingUtils.subtractNullable;

public class ListIndexRange extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;

    public ListIndexRange(boolean isInverted, Integer start, Integer end) {
        super(ListPartType.INDEX_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    public static ListIndexRange from(ConditionParser.ListIndexRangeContext ctx) {
        ConditionParser.StandardListIndexRangeContext indexRange = ctx.standardListIndexRange();
        ConditionParser.InvertedListIndexRangeContext invertedIndexRange = ctx.invertedListIndexRange();

        if (indexRange != null || invertedIndexRange != null) {
            ConditionParser.IndexRangeIdentifierContext range =
                    indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
            boolean isInverted = indexRange == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.end() != null) {
                end = parseSignedInt(range.end().signedInt());
            }

            return new ListIndexRange(isInverted, start, end);
        }
        throw new AelParseException("Could not translate ListIndexRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return ListExp.getByIndexRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return ListExp.getByIndexRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
