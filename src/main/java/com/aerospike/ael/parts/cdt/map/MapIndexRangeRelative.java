package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;

import com.aerospike.ael.util.ParsingUtils;

import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;
import static com.aerospike.ael.util.ParsingUtils.subtractNullable;

public class MapIndexRangeRelative extends MapPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public MapIndexRangeRelative(boolean isInverted, Integer start, Integer end, Object relative) {
        super(MapPartType.INDEX_RANGE_RELATIVE);
        if (relative != null) {
            requireSupportedKeyType(relative, "MapIndexRangeRelative");
        }
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    public static MapIndexRangeRelative from(ConditionParser.MapIndexRangeRelativeContext ctx) {
        ConditionParser.StandardMapIndexRangeRelativeContext indexRangeRelative = ctx.standardMapIndexRangeRelative();
        ConditionParser.InvertedMapIndexRangeRelativeContext invertedIndexRangeRelative = ctx.invertedMapIndexRangeRelative();

        if (indexRangeRelative != null || invertedIndexRangeRelative != null) {
            ConditionParser.IndexRangeRelativeIdentifierContext range =
                    indexRangeRelative != null ? indexRangeRelative.indexRangeRelativeIdentifier()
                            : invertedIndexRangeRelative.indexRangeRelativeIdentifier();
            boolean isInverted = indexRangeRelative == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.relativeKeyEnd().end() != null) {
                end = parseSignedInt(range.relativeKeyEnd().end().signedInt());
            }

            Object relativeKey = null;
            if (range.relativeKeyEnd().mapKey() != null) {
                relativeKey = ParsingUtils.parseMapKey(range.relativeKeyEnd().mapKey());
            }
            return new MapIndexRangeRelative(isInverted, start, end, relativeKey);
        }
        throw new AelParseException("Could not translate MapIndexRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp keyExp = ParsingUtils.objectToExp(relative);
        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByKeyRelativeIndexRange(cdtReturnType, keyExp, startExp,
                    Exp.bin(basePath.getBinPart().getBinName(),
                            basePath.getBinType()), context);
        }

        return MapExp.getByKeyRelativeIndexRange(cdtReturnType, keyExp, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
