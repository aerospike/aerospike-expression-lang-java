package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;

import com.aerospike.ael.util.ParsingUtils;

import java.util.Optional;

public class MapKeyRange extends MapPart {
    private final boolean isInverted;
    private final String start;
    private final String end;

    public MapKeyRange(boolean isInverted, String start, String end) {
        super(MapPartType.KEY_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.end = end;
    }

    public static MapKeyRange from(ConditionParser.MapKeyRangeContext ctx) {
        ConditionParser.StandardMapKeyRangeContext keyRange = ctx.standardMapKeyRange();
        ConditionParser.InvertedMapKeyRangeContext invertedKeyRange = ctx.invertedMapKeyRange();

        if (keyRange != null || invertedKeyRange != null) {
            ConditionParser.KeyRangeIdentifierContext range =
                    keyRange != null ? keyRange.keyRangeIdentifier() : invertedKeyRange.keyRangeIdentifier();
            boolean isInverted = keyRange == null;

            String startKey = ParsingUtils.parseMapKey(range.mapKey(0));

            String endKey = Optional.ofNullable(range.mapKey(1))
                    .map(ParsingUtils::parseMapKey)
                    .orElse(null);

            return new MapKeyRange(isInverted, startKey, endKey);
        }
        throw new AelParseException("Could not translate MapKeyRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        Exp endExp = end != null ? Exp.val(end) : null;

        return MapExp.getByKeyRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
