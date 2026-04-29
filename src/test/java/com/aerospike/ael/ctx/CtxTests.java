package com.aerospike.ael.ctx;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.Value;
import com.aerospike.ael.client.cdt.CTX;
import org.junit.jupiter.api.Test;

import static com.aerospike.ael.util.TestUtils.parseCtx;
import static com.aerospike.ael.util.TestUtils.parseCtxAndCompareAsBase64;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CtxTests {

    @Test
    void listExpression_onlyBin_noCtx() {
        assertThatThrownBy(() -> parseCtx("$.listBin1"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("CDT context is not provided");
    }

    @Test
    void listExpression_emptyOrMalformedInput() {
        assertThatThrownBy(() -> parseCtx(null))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Path must not be null or empty");

        assertThatThrownBy(() -> parseCtx(""))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Path must not be null or empty");
        assertThatThrownBy(() -> parseCtx("$..listBin1"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input");
        assertThatThrownBy(() -> parseCtx("$listBin1"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input");
    }

    @Test
    void listExpression_oneLevel() {
        parseCtxAndCompareAsBase64("$.listBin1.[0]",
                new CTX[]{CTX.listIndex(0)});
        parseCtxAndCompareAsBase64("$.listBin1.[=100]",
                new CTX[]{CTX.listValue(Value.get(100))});
        parseCtxAndCompareAsBase64("$.listBin1.[#-1]",
                new CTX[]{CTX.listRank(-1)});
    }

    @Test
    void listExpression_oneLevel_withPathFunction() {
        assertThatThrownBy(() -> parseCtx("$.listBin1.[0].get(type: INT)"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.listBin1.[=100].get(type: INT, return: VALUE)"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.listBin1.[#-1].asInt()"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
    }

    @Test
    void listExpression_oneLevel_withFullAelExpression() {
        assertThatThrownBy(() -> parseCtx("$.listBin1.[0] == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.listBin1.[=100].get(type: INT, return: VALUE) == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.listBin1.[#-1].asInt() == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
    }

    @Test
    void listExpression_twoLevels() {
        parseCtxAndCompareAsBase64("$.listBin1.[0].[1]",
                new CTX[]{CTX.listIndex(0), CTX.listIndex(1)});
        parseCtxAndCompareAsBase64("$.listBin1.[0].[=100]",
                new CTX[]{CTX.listIndex(0), CTX.listValue(Value.get(100))});
        parseCtxAndCompareAsBase64("$.listBin1.[#-1].[=100]",
                new CTX[]{CTX.listRank(-1), CTX.listValue(Value.get(100))});
    }

    @Test
    void listExpression_threeLevels() {
        parseCtxAndCompareAsBase64("$.listBin1.[0].[1].[2]",
                new CTX[]{CTX.listIndex(0), CTX.listIndex(1), CTX.listIndex(2)});
        parseCtxAndCompareAsBase64("$.listBin1.[#-1].[0].[=100]",
                new CTX[]{CTX.listRank(-1), CTX.listIndex(0), CTX.listValue(Value.get(100))});
        parseCtxAndCompareAsBase64("$.listBin1.[#-1].[=100].[0]",
                new CTX[]{CTX.listRank(-1), CTX.listValue(Value.get(100)), CTX.listIndex(0)});
    }

    @Test
    void mapExpression_onlyBin_noCtx() {
        assertThatThrownBy(() -> parseCtx("$.mapBin1"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("CDT context is not provided");
    }

    @Test
    void mapExpression_oneLevel() {
        parseCtxAndCompareAsBase64("$.mapBin1.a",
                new CTX[]{CTX.mapKey(Value.get("a"))});
        parseCtxAndCompareAsBase64("$.mapBin1.{0}",
                new CTX[]{CTX.mapIndex(0)});
        parseCtxAndCompareAsBase64("$.mapBin1.{#-1}",
                new CTX[]{CTX.mapRank(-1)});
        parseCtxAndCompareAsBase64("$.mapBin1.{=100}",
                new CTX[]{CTX.mapValue(Value.get(100))});
    }

    @Test
    void mapExpression_halfOpenIndexRangeUnsupportedForCtx() {
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{:3}"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Context is not supported");
    }

    @Test
    void listExpression_halfOpenIndexRangeUnsupportedForCtx() {
        assertThatThrownBy(() -> parseCtx("$.listBin1.[:3]"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Context is not supported");
    }

    @Test
    void mapExpression_oneLevel_withPathFunction() {
        assertThatThrownBy(() -> parseCtx("$.mapBin1.a.get(type: INT)"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{0}.get(type: INT)"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{=100}.get(type: INT, return: VALUE)"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{#-1}.asInt()"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Path function is unsupported, please provide only path to convert to CTX[]");
    }

    @Test
    void mapExpression_oneLevel_withFullAelExpression() {
        assertThatThrownBy(() -> parseCtx("$.mapBin1.a == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{0}.get(type: INT, return: VALUE) == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{=100}.asInt() == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
        assertThatThrownBy(() -> parseCtx("$.mapBin1.{#-1}.asInt() == 100"))
                .isInstanceOf(AelParseException.class)
                .hasMessageContaining("Could not parse the given AEL path input")
                .hasStackTraceContaining("Unsupported input expression type 'EXPRESSION_CONTAINER', " +
                        "please provide only path to convert to CTX[]");
    }

    @Test
    void mapExpression_twoLevels() {
        parseCtxAndCompareAsBase64("$.mapBin1.{0}.a",
                new CTX[]{CTX.mapIndex(0), CTX.mapKey(Value.get("a"))});
        parseCtxAndCompareAsBase64("$.mapBin1.{0}.{=100}",
                new CTX[]{CTX.mapIndex(0), CTX.mapValue(Value.get(100))});
        parseCtxAndCompareAsBase64("$.mapBin1.{#-1}.{=100}",
                new CTX[]{CTX.mapRank(-1), CTX.mapValue(Value.get(100))});
    }

    @Test
    void mapExpression_threeLevels() {
        parseCtxAndCompareAsBase64("$.mapBin1.{0}.a.{#-1}",
                new CTX[]{CTX.mapIndex(0), CTX.mapKey(Value.get("a")), CTX.mapRank(-1)});
        parseCtxAndCompareAsBase64("$.mapBin1.{0}.{=100}.a",
                new CTX[]{CTX.mapIndex(0), CTX.mapValue(Value.get(100)), CTX.mapKey(Value.get("a"))});
        parseCtxAndCompareAsBase64("$.mapBin1.{=100}.{#-1}.{0}",
                new CTX[]{CTX.mapValue(Value.get(100)), CTX.mapRank(-1), CTX.mapIndex(0)});
    }

    @Test
    void combinedListMapExpression_fourLevels() {
        parseCtxAndCompareAsBase64("$.mapBin1.{0}.a.{#-1}.[=100]",
                new CTX[]{CTX.mapIndex(0), CTX.mapKey(Value.get("a")), CTX.mapRank(-1), CTX.listValue(Value.get(100))});
        parseCtxAndCompareAsBase64("$.listBin1.[0].[=100].a.{0}",
                new CTX[]{CTX.listIndex(0), CTX.listValue(Value.get(100)), CTX.mapKey(Value.get("a")), CTX.mapIndex(0)});
        parseCtxAndCompareAsBase64("$.mapBin1.{=100}.[#-1].{#-1}.[0]",
                new CTX[]{CTX.mapValue(Value.get(100)), CTX.listRank(-1), CTX.mapRank(-1), CTX.listIndex(0)});
    }

    // ---- BLOB in parseCTX ----

    @Test
    void parseCTXWithBlobValue() {
        parseCtxAndCompareAsBase64("$.bin.[=X'ff00']",
                new CTX[]{CTX.listValue(Value.get(new byte[]{(byte) 0xff, 0x00}))});
    }

    @Test
    void parseCTXWithB64BlobValue() {
        parseCtxAndCompareAsBase64("$.bin.[=b64'AQID']",
                new CTX[]{CTX.listValue(Value.get(new byte[]{1, 2, 3}))});
    }

    // ---- New map key type tests ----

    @Test
    void ctxDigitOnlyMapKey() {
        parseCtxAndCompareAsBase64("$.bin.55",
                new CTX[]{CTX.mapKey(Value.get(55L))});
    }

    @Test
    void ctxNegativeSignedMapKey() {
        parseCtxAndCompareAsBase64("$.bin.-100",
                new CTX[]{CTX.mapKey(Value.get(-100L))});
    }

    @Test
    void ctxPositiveSignedMapKey() {
        parseCtxAndCompareAsBase64("$.bin.+100",
                new CTX[]{CTX.mapKey(Value.get(100L))});
    }

    @Test
    void ctxHexMapKey() {
        parseCtxAndCompareAsBase64("$.bin.0xff",
                new CTX[]{CTX.mapKey(Value.get(255L))});
    }

    @Test
    void ctxBinaryMapKey() {
        parseCtxAndCompareAsBase64("$.bin.0b1010",
                new CTX[]{CTX.mapKey(Value.get(10L))});
    }

    @Test
    void ctxMixedMapKeyTypes() {
        parseCtxAndCompareAsBase64("$.bin.55.key.-1.0xff",
                new CTX[]{CTX.mapKey(Value.get(55L)), CTX.mapKey(Value.get("key")),
                        CTX.mapKey(Value.get(-1L)), CTX.mapKey(Value.get(255L))});
    }

    @Test
    void ctxAtBinWithMapKey() {
        parseCtxAndCompareAsBase64("$.name@host.key",
                new CTX[]{CTX.mapKey(Value.get("key"))});
    }

    @Test
    void ctxAtBinWithListIndex() {
        parseCtxAndCompareAsBase64("$.@attr.[0]",
                new CTX[]{CTX.listIndex(0)});
    }

    @Test
    void ctxLeadingZeroMapKey() {
        parseCtxAndCompareAsBase64("$.bin.007",
                new CTX[]{CTX.mapKey(Value.get(7L))});
    }

    @Test
    void ctxHexBlobMapKey() {
        parseCtxAndCompareAsBase64("$.bin.X'ff00'",
                new CTX[]{CTX.mapKey(Value.get(new byte[]{(byte) 0xff, 0x00}))});
    }

    @Test
    void ctxB64BlobMapKey() {
        parseCtxAndCompareAsBase64("$.bin.b64'AQID'",
                new CTX[]{CTX.mapKey(Value.get(new byte[]{1, 2, 3}))});
    }

    @Test
    void ctxMixedWithBlobMapKey() {
        parseCtxAndCompareAsBase64("$.bin.X'ff'.key.-1",
                new CTX[]{CTX.mapKey(Value.get(new byte[]{(byte) 0xff})),
                        CTX.mapKey(Value.get("key")), CTX.mapKey(Value.get(-1L))});
    }

    @Test
    void ctxBlobKeyWithMapIndex() {
        parseCtxAndCompareAsBase64("$.bin.X'aa'.{0}",
                new CTX[]{CTX.mapKey(Value.get(new byte[]{(byte) 0xaa})),
                        CTX.mapIndex(0)});
    }
}
