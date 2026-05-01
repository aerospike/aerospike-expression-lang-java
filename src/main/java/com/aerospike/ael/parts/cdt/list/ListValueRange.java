package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.parts.path.BasePath;

import com.aerospike.ael.util.ParsingUtils;

import static com.aerospike.ael.util.ParsingUtils.objectToExp;
import static com.aerospike.ael.util.ParsingUtils.requireSupportedExpValue;

public class ListValueRange extends ListPart {
    private final boolean isInverted;
    private final Object start;
    private final Object end;

    public ListValueRange(boolean isInverted, Object start, Object end) {
        super(ListPartType.VALUE_RANGE);
        if (start != null) {
            requireSupportedExpValue(start, "ListValueRange start");
        }
        if (end != null) {
            requireSupportedExpValue(end, "ListValueRange end");
        }
        if (start == null && end == null) {
            throw new AelParseException("ListValueRange requires at least one bound");
        }
        this.isInverted = isInverted;
        this.start = start;
        this.end = end;
    }

    public static ListValueRange from(ConditionParser.ListValueRangeContext ctx) {
        ConditionParser.StandardListValueRangeContext valueRange = ctx.standardListValueRange();
        ConditionParser.InvertedListValueRangeContext invertedValueRange = ctx.invertedListValueRange();

        if (valueRange != null || invertedValueRange != null) {
            ConditionParser.ValueRangeIdentifierContext range =
                    valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
            boolean isInverted = valueRange == null;

            ParsingUtils.NullableEndpoints<Object> bounds = ParsingUtils.parseValueRangeEndpoints(range);
            return new ListValueRange(isInverted, bounds.start(), bounds.end());
        }
        throw new AelParseException("Could not translate ListValueRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp startExp = start != null ? objectToExp(start) : null;
        Exp endExp = end != null ? objectToExp(end) : null;

        return ListExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
