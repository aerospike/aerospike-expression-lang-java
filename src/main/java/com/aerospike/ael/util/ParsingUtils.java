package com.aerospike.ael.util;

import com.aerospike.ael.ConditionParser;
import com.aerospike.ael.AelParseException;
import com.aerospike.ael.client.exp.Exp;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.math.BigInteger;

@UtilityClass
public class ParsingUtils {

    private static final BigInteger LONG_MIN_ABS = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    private static final BigInteger INT_MIN_VALUE = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT_MAX_VALUE = BigInteger.valueOf(Integer.MAX_VALUE);

    /**
     * Extracts a signed integer value from a {@code signedInt} parser rule context.
     * The grammar rule is {@code signedInt: '-'? INT;}, so the context contains
     * either just an INT token or a '-' followed by an INT token.
     *
     * @param ctx The signedInt context from the parser
     * @return The parsed integer value, negated if a '-' prefix is present
     */
    public static int parseSignedInt(ConditionParser.SignedIntContext ctx) {
        String intText = ctx.INT().getText();
        boolean isNegative = ctx.getText().startsWith("-");
        if (isHexOrBinaryIntToken(intText)) {
            throw new AelParseException("Only decimal integer literals are supported in this element: " + ctx.getText());
        }

        BigInteger signedValue = getBigInteger(ctx, intText, isNegative);

        return signedValue.intValue();
    }

    private static BigInteger getBigInteger(ConditionParser.SignedIntContext ctx, String intText, boolean isNegative) {
        BigInteger value;
        try {
            value = new BigInteger(intText, 10);
        } catch (NumberFormatException e) {
            throw new AelParseException("Invalid integer literal: " + ctx.getText(), e);
        }
        BigInteger signedValue = isNegative ? value.negate() : value;

        if (signedValue.compareTo(INT_MIN_VALUE) < 0 || signedValue.compareTo(INT_MAX_VALUE) > 0) {
            throw new AelParseException("Signed integer literal out of range for INT: " + ctx.getText());
        }
        return signedValue;
    }

    private static boolean isHexOrBinaryIntToken(String intText) {
        return intText.length() > 2
                && intText.charAt(0) == '0'
                && (intText.charAt(1) == 'x' || intText.charAt(1) == 'X'
                || intText.charAt(1) == 'b' || intText.charAt(1) == 'B');
    }

    /**
     * Parses an unsigned INT token (decimal, hex or binary) into a long value.
     * The value range is [0, 2^63], where 2^63 is represented as {@link Long#MIN_VALUE}.
     *
     * @param text INT token text
     * @return Parsed long value (unsigned 2^63 maps to {@link Long#MIN_VALUE})
     */
    public static long parseUnsignedLongLiteral(String text) {
        BigInteger value = parseUnsignedIntegerLiteral(text);
        if (value.compareTo(LONG_MIN_ABS) > 0) {
            throw new AelParseException("Integer literal out of range: " + text);
        }
        return value.longValue();
    }

    private static BigInteger parseUnsignedIntegerLiteral(String text) {
        try {
            int radix = 10;
            String digits = text;
            if (text.length() > 2 && text.charAt(0) == '0') {
                char prefix = text.charAt(1);
                if (prefix == 'x' || prefix == 'X') {
                    radix = 16;
                    digits = text.substring(2);
                } else if (prefix == 'b' || prefix == 'B') {
                    radix = 2;
                    digits = text.substring(2);
                }
            }
            return new BigInteger(digits, radix);
        } catch (NumberFormatException e) {
            throw new AelParseException("Invalid integer literal: " + text, e);
        }
    }

    /**
     * Resolves the string content from a parser rule context that may contain
     * NAME_IDENTIFIER, QUOTED_STRING, or IN tokens.
     *
     * @param ctx Any parser rule context containing string-like tokens
     * @return The resolved string, or {@code null} if no matching token is found
     */
    private static String resolveStringToken(ParserRuleContext ctx) {
        TerminalNode nameId = ctx.getToken(ConditionParser.NAME_IDENTIFIER, 0);
        if (nameId != null) {
            return nameId.getText();
        }
        TerminalNode quoted = ctx.getToken(ConditionParser.QUOTED_STRING, 0);
        if (quoted != null) {
            return unquote(quoted.getText());
        }
        TerminalNode in = ctx.getToken(ConditionParser.IN, 0);
        if (in != null) {
            return in.getText();
        }
        return null;
    }

    /**
     * Extracts a typed value from a {@code mapKey} parser rule context.
     * Returns {@link Long} for pure decimal INT tokens, {@link String} for hex/binary INT tokens,
     * and {@link String} for NAME_IDENTIFIER, QUOTED_STRING, and IN keyword.
     *
     * @param ctx The mapKey context from the parser
     * @return The parsed key as String, Long, or byte[]
     */
    public static Object parseMapKey(ConditionParser.MapKeyContext ctx) {
        String result = resolveStringToken(ctx);
        if (result != null) {
            return result;
        }
        TerminalNode intToken = ctx.getToken(ConditionParser.INT, 0);
        if (intToken != null) {
            String intText = intToken.getText();
            if (isHexOrBinaryIntToken(intText)) {
                return intText;
            }
            return parseLongMapKey(intText);
        }
        TerminalNode blobLiteral = ctx.getToken(ConditionParser.BLOB_LITERAL, 0);
        if (blobLiteral != null) {
            return parseHexToBytes(blobLiteral.getText());
        }
        TerminalNode b64Literal = ctx.getToken(ConditionParser.B64_LITERAL, 0);
        if (b64Literal != null) {
            return parseB64ToBytes(b64Literal.getText());
        }
        throw new AelParseException("Could not parse mapKey from ctx: %s".formatted(ctx.getText()));
    }

    /**
     * Parses a decimal digit string as a long map key, wrapping overflow in {@link AelParseException}.
     */
    public static long parseLongMapKey(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new AelParseException("Integer map key out of range: " + text, e);
        }
    }

    /**
     * Extracts a typed value from a {@code valueIdentifier} parser rule context.
     * Handles NAME_IDENTIFIER, QUOTED_STRING, IN keyword (as literal text), and signedInt.
     *
     * @param ctx The valueIdentifier context from the parser
     * @return The parsed value as String or Integer
     */
    public static Object parseValueIdentifier(ConditionParser.ValueIdentifierContext ctx) {
        String result = resolveStringToken(ctx);
        if (result != null) {
            return result;
        }
        if (ctx.signedInt() != null) {
            return parseSignedInt(ctx.signedInt());
        }
        TerminalNode blobLiteral = ctx.getToken(ConditionParser.BLOB_LITERAL, 0);
        if (blobLiteral != null) {
            return parseHexToBytes(blobLiteral.getText());
        }
        TerminalNode b64Literal = ctx.getToken(ConditionParser.B64_LITERAL, 0);
        if (b64Literal != null) {
            return parseB64ToBytes(b64Literal.getText());
        }
        throw new AelParseException("Could not parse valueIdentifier from ctx: %s".formatted(ctx.getText()));
    }

    // Token format: X'hexchars' or x'hexchars' — strip 2-char prefix and trailing quote
    public static byte[] parseHexToBytes(String text) {
        String hex = text.substring(2, text.length() - 1);
        if (hex.length() % 2 != 0) {
            throw new AelParseException(
                    "BLOB literal must contain an even number of hex characters: " + text);
        }
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    // Token format: b64'base64chars' or B64'base64chars' — strip 4-char prefix and trailing quote
    public static byte[] parseB64ToBytes(String text) {
        String b64 = text.substring(4, text.length() - 1);
        try {
            return java.util.Base64.getDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            throw new AelParseException(
                    "Base64 BLOB literal contains invalid Base64 content: " + text, e);
        }
    }

    /**
     * Parses a {@code valueIdentifier} context and requires the result to be an {@link Integer}.
     * Used by value-range elements where only integer operands are valid.
     *
     * @param ctx The valueIdentifier context from the parser
     * @return The parsed integer value
     * @throws AelParseException if the parsed value is not an integer
     */
    public static Integer requireIntValueIdentifier(ConditionParser.ValueIdentifierContext ctx) {
        Object result = parseValueIdentifier(ctx);
        if (result instanceof Integer intValue) {
            return intValue;
        }
        throw new AelParseException(
                "Value range requires integer operands, got: %s".formatted(ctx.getText()));
    }

    /**
     * Converts a parsed value object to an {@link Exp} value expression.
     * Supports {@link String}, {@link Long}, {@link Integer}, and {@code byte[]}.
     *
     * @param value The parsed value object
     * @return The corresponding {@link Exp} value expression
     * @throws AelParseException if the value type is not supported
     */
    public static Exp objectToExp(Object value) {
        if (value instanceof String s) return Exp.val(s);
        if (value instanceof Long l) return Exp.val(l);
        if (value instanceof Integer i) return Exp.val(i);
        if (value instanceof byte[] b) return Exp.val(b);
        throw new AelParseException(
                "Unsupported value type for Exp conversion: " + value.getClass().getSimpleName());
    }

    /**
     * Get the string inside the quotes.
     *
     * @param str String input
     * @return String inside the quotes
     */
    public static String unquote(String str) {
        if (str.length() > 2) {
            return str.substring(1, str.length() - 1);
        } else {
            throw new IllegalArgumentException("String %s must contain more than 2 characters".formatted(str));
        }
    }

    /**
     * @param a Integer, can be null
     * @param b Integer, non-null
     * @return a - b if a != null, otherwise null
     */
    public static Integer subtractNullable(Integer a, @NonNull Integer b) {
        return a == null ? null : a - b;
    }

    /**
     * Extracts the type string from a method name expected to start with "as" and end with "()".
     *
     * @param methodName The method name string
     * @return The extracted type string
     * @throws AelParseException if the method name is not in the correct format
     */
    public static String extractTypeFromMethod(String methodName) {
        if (methodName.startsWith("as") && methodName.endsWith("()")) {
            return methodName.substring(2, methodName.length() - 2);
        } else {
            throw new AelParseException("Invalid method name: %s".formatted(methodName));
        }
    }

    /**
     * Extracts the function name from a string that may include parameters in parentheses.
     *
     * @param text The input string containing the function name and potentially parameters
     * @return The extracted function name
     */
    public static String extractFunctionName(String text) {
        int startParen = text.indexOf('(');
        return (startParen != -1) ? text.substring(0, startParen) : text;
    }

    /**
     * Extracts an integer parameter from a string enclosed in parentheses.
     *
     * @param text The input string
     * @return The extracted integer parameter, or {@code null} if not found or invalid
     */
    public static Integer extractParameter(String text) {
        int startParen = text.indexOf('(');
        int endParen = text.indexOf(')');

        if (startParen != -1 && endParen != -1 && endParen > startParen + 1) {
            String numberStr = text.substring(startParen + 1, endParen);
            return Integer.parseInt(numberStr);
        }
        return null;
    }
}
