package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.cdt.MapReturnType;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;

import com.aerospike.ael.util.ParsingUtils;

import java.util.List;

public class MapValueList extends MapPart {
    private final boolean isInverted;
    private final List<?> valueList;

    public MapValueList(boolean isInverted, List<?> valueList) {
        super(MapPartType.VALUE_LIST);
        this.isInverted = isInverted;
        this.valueList = valueList;
    }

    public static MapValueList from(ConditionParser.MapValueListContext ctx) {
        ConditionParser.StandardMapValueListContext valueList = ctx.standardMapValueList();
        ConditionParser.InvertedMapValueListContext invertedValueList = ctx.invertedMapValueList();

        if (valueList != null || invertedValueList != null) {
            ConditionParser.ValueListIdentifierContext list =
                    valueList != null ? valueList.valueListIdentifier() : invertedValueList.valueListIdentifier();
            boolean isInverted = valueList == null;

            List<?> valueListObjects = list.valueIdentifier().stream()
                    .map(ParsingUtils::parseValueIdentifier)
                    .toList();

            return new MapValueList(isInverted, valueListObjects);
        }
        throw new AelParseException("Could not translate MapValueList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        return MapExp.getByValueList(cdtReturnType, Exp.val(valueList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
