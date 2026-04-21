package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.util.ParsingUtils;

import static com.aerospike.ael.util.ParsingUtils.objectToExp;
import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;
import static com.aerospike.ael.util.ParsingUtils.subtractNullable;

public class MapRankRangeRelative extends MapPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public MapRankRangeRelative(boolean isInverted, Integer start, Integer end, Object relative) {
        super(MapPartType.RANK_RANGE_RELATIVE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    public static MapRankRangeRelative from(ConditionParser.MapRankRangeRelativeContext ctx) {
        ConditionParser.StandardMapRankRangeRelativeContext rankRangeRelative = ctx.standardMapRankRangeRelative();
        ConditionParser.InvertedMapRankRangeRelativeContext invertedRankRangeRelative = ctx.invertedMapRankRangeRelative();

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

            return new MapRankRangeRelative(isInverted, start, end, relativeValue);
        }
        throw new AelParseException("Could not translate MapRankRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp relativeExp = objectToExp(relative);

        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
        }

        return MapExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
