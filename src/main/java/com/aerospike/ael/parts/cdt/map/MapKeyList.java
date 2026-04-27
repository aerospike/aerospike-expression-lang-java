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

public class MapKeyList extends MapPart {
    private final boolean isInverted;
    private final List<Object> keyList;

    public MapKeyList(boolean isInverted, List<Object> keyList) {
        super(MapPartType.KEY_LIST);
        keyList.forEach(k -> requireSupportedKeyType(k, "MapKeyList"));
        this.isInverted = isInverted;
        this.keyList = keyList;
    }

    public static MapKeyList from(ConditionParser.MapKeyListContext ctx) {
        ConditionParser.StandardMapKeyListContext keyList = ctx.standardMapKeyList();
        ConditionParser.InvertedMapKeyListContext invertedKeyList = ctx.invertedMapKeyList();

        if (keyList != null || invertedKeyList != null) {
            ConditionParser.KeyListIdentifierContext list =
                    keyList != null ? keyList.keyListIdentifier() : invertedKeyList.keyListIdentifier();
            boolean isInverted = keyList == null;

            List<Object> keyListValues = list.mapKey().stream()
                    .map(ParsingUtils::parseMapKey)
                    .toList();

            return new MapKeyList(isInverted, keyListValues);
        }
        throw new AelParseException("Could not translate MapKeyList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        return MapExp.getByKeyList(cdtReturnType, Exp.val(keyList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
