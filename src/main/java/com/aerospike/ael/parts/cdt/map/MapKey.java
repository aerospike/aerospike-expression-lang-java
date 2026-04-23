package com.aerospike.ael.parts.cdt.map;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.client.Value;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.util.ParsingUtils;

public class MapKey extends MapPart {
    private final Object key;

    public MapKey(Object key) {
        super(MapPartType.KEY);
        requireStringOrLong(key, "MapKey");
        this.key = key;
    }

    public static MapKey from(ConditionParser.MapKeyContext ctx) {
        return new MapKey(ParsingUtils.parseMapKey(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByKey(cdtReturnType, valueType,
                ParsingUtils.objectToExp(key),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapKey(Value.get(key));
    }
}
