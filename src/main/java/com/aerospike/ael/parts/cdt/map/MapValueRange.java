package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;

import static com.aerospike.ael.util.ParsingUtils.requireIntValueIdentifier;

public class MapValueRange extends MapPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer end;

    public MapValueRange(boolean isInverted, Integer start, Integer end) {
        super(MapPartType.VALUE_RANGE);
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

            Integer startValue = requireIntValueIdentifier(range.valueIdentifier(0));

            Integer endValue = null;
            if (range.valueIdentifier(1) != null) {
                endValue = requireIntValueIdentifier(range.valueIdentifier(1));
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

        Exp startExp = Exp.val(start);
        Exp endExp = end != null ? Exp.val(end) : null;

        return MapExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
