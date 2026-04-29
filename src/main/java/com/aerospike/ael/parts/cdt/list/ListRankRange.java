package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.parts.path.BasePath;

import static com.aerospike.ael.util.ParsingUtils.halfOpenRangeCount;
import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;

public class ListRankRange extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;

    public ListRankRange(boolean isInverted, Integer start, Integer end) {
        super(ListPartType.RANK_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = halfOpenRangeCount(start, end);
    }

    public static ListRankRange from(ConditionParser.ListRankRangeContext ctx) {
        ConditionParser.StandardListRankRangeContext rankRange = ctx.standardListRankRange();
        ConditionParser.InvertedListRankRangeContext invertedRankRange = ctx.invertedListRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = range.start() != null ? parseSignedInt(range.start().signedInt()) : null;
            Integer end = range.end() != null ? parseSignedInt(range.end().signedInt()) : null;

            return new ListRankRange(isInverted, start, end);
        }
        throw new AelParseException("Could not translate ListRankRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        int startRank = start != null ? start : 0;
        Exp startExp = Exp.val(startRank);
        if (count == null) {
            return ListExp.getByRankRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return ListExp.getByRankRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
