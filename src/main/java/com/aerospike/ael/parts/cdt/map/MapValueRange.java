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
import static com.aerospike.ael.util.ParsingUtils.requireSupportedExpValue;

public class MapValueRange extends MapPart {
    private final boolean isInverted;
    private final Object start;
    private final Object end;

    public MapValueRange(boolean isInverted, Object start, Object end) {
        super(MapPartType.VALUE_RANGE);
        if (start != null) {
            requireSupportedExpValue(start, "MapValueRange start");
        }
        if (end != null) {
            requireSupportedExpValue(end, "MapValueRange end");
        }
        if (start == null && end == null) {
            throw new AelParseException("MapValueRange requires at least one bound");
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

            ParsingUtils.NullableEndpoints<Object> bounds = ParsingUtils.parseValueRangeEndpoints(range);
            return new MapValueRange(isInverted, bounds.start(), bounds.end());
        }
        throw new AelParseException("Could not translate MapValueRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = start != null ? objectToExp(start) : null;
        Exp endExp = end != null ? objectToExp(end) : null;

        return MapExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
