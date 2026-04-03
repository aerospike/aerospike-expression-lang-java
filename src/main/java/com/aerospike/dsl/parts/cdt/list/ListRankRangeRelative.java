package com.aerospike.dsl.parts.cdt.list;

import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.client.cdt.CTX;
import com.aerospike.dsl.client.cdt.ListReturnType;
import com.aerospike.dsl.client.exp.Exp;
import com.aerospike.dsl.client.exp.ListExp;
import com.aerospike.dsl.parts.path.BasePath;
import com.aerospike.dsl.util.ParsingUtils;

import static com.aerospike.dsl.util.ParsingUtils.objectToExp;
import static com.aerospike.dsl.util.ParsingUtils.parseSignedInt;
import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

public class ListRankRangeRelative extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public ListRankRangeRelative(boolean isInverted, Integer start, Integer end, Object relative) {
        super(ListPartType.RANK_RANGE_RELATIVE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
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

            Integer start = parseSignedInt(range.start().signedInt());
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
        throw new DslParseException("Could not translate ListRankRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp relativeExp = objectToExp(relative);

        Exp startExp = Exp.val(start);
        if (count == null) {
            return ListExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(),
                            basePath.getBinType()), context);
        }

        return ListExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
