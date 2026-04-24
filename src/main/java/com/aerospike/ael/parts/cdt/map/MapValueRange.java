package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;

import static com.aerospike.ael.util.ParsingUtils.objectToExp;
import static com.aerospike.ael.util.ParsingUtils.parseValueIdentifier;
import static com.aerospike.ael.util.ParsingUtils.requireSupportedExpValue;

public class MapValueRange extends MapPart {
    private final boolean isInverted;
    private final Object start;
    private final Object end;

    public MapValueRange(boolean isInverted, Object start, Object end) {
        super(MapPartType.VALUE_RANGE);
        requireSupportedExpValue(start, "MapValueRange start");
        if (end != null) {
            requireSupportedExpValue(end, "MapValueRange end");
        }
        this.isInverted = isInverted;
        this.start = start;
        this.end = end;
    }

    public static MapValueRange from(ConditionParser.MapValueRangeContext ctx) {
        ConditionParser.StandardMapValueRangeContext valueRange = ctx.standardMapValueRange();
        ConditionParser.InvertedMapValueRangeContext invertedValueRange = ctx.invertedMapValueRange();

        if (valueRange != null || invertedValueRange != null) {
            ConditionParser.ValueRangeIdentifierContext range =
                    valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
            boolean isInverted = valueRange == null;

            Object startValue = parseValueIdentifier(range.valueIdentifier(0));

            Object endValue = null;
            if (range.valueIdentifier(1) != null) {
                endValue = parseValueIdentifier(range.valueIdentifier(1));
            }

            return new MapValueRange(isInverted, startValue, endValue);
        }
        throw new AelParseException("Could not translate MapValueRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = objectToExp(start);
        Exp endExp = end != null ? objectToExp(end) : null;

        return MapExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
