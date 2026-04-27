package com.aerospike.ael.parts.cdt.list;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.ListReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.parts.path.BasePath;

import static com.aerospike.ael.util.ParsingUtils.requireIntValueIdentifier;

public class ListValueRange extends ListPart {
    private final boolean isInverted;
    private final Object start;
    private final Object end;

    public ListValueRange(boolean isInverted, Object start, Object end) {
        super(ListPartType.VALUE_RANGE);
        requireSupportedExpValue(start, "ListValueRange start");
        if (end != null) {
            requireSupportedExpValue(end, "ListValueRange end");
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

            Object startValue = parseValueIdentifier(range.valueIdentifier(0));

            Object endValue = null;
            if (range.valueIdentifier(1) != null) {
                endValue = parseValueIdentifier(range.valueIdentifier(1));
            }

            return new ListValueRange(isInverted, startValue, endValue);
        }
        throw new AelParseException("Could not translate ListValueRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp startExp = objectToExp(start);
        Exp endExp = end != null ? objectToExp(end) : null;

        return ListExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
