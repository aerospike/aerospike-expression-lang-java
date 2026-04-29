package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.util.ParsingUtils;

import static com.aerospike.ael.util.ParsingUtils.halfOpenRangeCount;
import static com.aerospike.ael.util.ParsingUtils.objectToExp;
import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;

public class ListRankRangeRelative extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public ListRankRangeRelative(boolean isInverted, Integer start, Integer end, Object relative) {
        super(ListPartType.RANK_RANGE_RELATIVE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = halfOpenRangeCount(start, end);
        this.relative = relative;
    }

    public static ListRankRangeRelative from(ConditionParser.ListRankRangeRelativeContext ctx) {
        ConditionParser.StandardListRankRangeRelativeContext rankRangeRelative = ctx.standardListRankRangeRelative();
        ConditionParser.InvertedListRankRangeRelativeContext invertedRankRangeRelative = ctx.invertedListRankRangeRelative();

        if (rankRangeRelative != null || invertedRankRangeRelative != null) {
            ConditionParser.RankRangeRelativeIdentifierContext range =
                    rankRangeRelative != null ? rankRangeRelative.rankRangeRelativeIdentifier()
                            : invertedRankRangeRelative.rankRangeRelativeIdentifier();
            boolean isInverted = rankRangeRelative == null;

            Integer start = range.start() != null ? parseSignedInt(range.start().signedInt()) : null;
            Integer end = null;
            if (range.relativeRankEnd().end() != null) {
                end = parseSignedInt(range.relativeRankEnd().end().signedInt());
            }

            Object relativeValue = null;
            if (range.relativeRankEnd().relativeValue() != null) {
                relativeValue = ParsingUtils.parseValueIdentifier(
                        range.relativeRankEnd().relativeValue().valueIdentifier());
            }

            return new ListRankRangeRelative(isInverted, start, end, relativeValue);
        }
        throw new AelParseException("Could not translate ListRankRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp relativeExp = objectToExp(relative);

        Exp valueExp = Exp.val(start != null ? start : 0);
        if (count == null) {
            return ListExp.getByValueRelativeRankRange(cdtReturnType, valueExp, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(),
                            basePath.getBinType()), context);
        }

        return ListExp.getByValueRelativeRankRange(cdtReturnType, valueExp, relativeExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
