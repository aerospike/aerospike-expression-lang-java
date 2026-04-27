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
    private final Integer start;
    private final Integer end;

    public ListValueRange(boolean isInverted, Integer start, Integer end) {
        super(ListPartType.VALUE_RANGE);
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

            Integer startValue = requireIntValueIdentifier(range.valueIdentifier(0));

            Integer endValue = null;
            if (range.valueIdentifier(1) != null) {
                endValue = requireIntValueIdentifier(range.valueIdentifier(1));
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

        Exp startExp = Exp.val(start);
        Exp endExp = end != null ? Exp.val(end) : null;

        return ListExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
