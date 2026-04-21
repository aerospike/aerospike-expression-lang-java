package com.aerospike.ael.impl;

import com.aerospike.ael.ConditionLexer;
import com.aerospike.ael.AelParseException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

class AelParserErrorListener extends BaseErrorListener {

    private String firstLexerError;
    private String firstParserError;

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg, RecognitionException e) {
        if (offendingSymbol instanceof Token token) {
            // LEADING_DOT_FLOAT_HEX_OR_BINARY (.0xff/.0b101) is never accepted by the grammar;
            // LEADING_DOT_FLOAT is the offending token in cases like ".3.7" where ".7" is lexed
            // as a valid-looking token but appears where an operator was expected.
            // These throw immediately to provide a specific error message.
            if (token.getType() == ConditionLexer.LEADING_DOT_FLOAT_HEX_OR_BINARY
                    || token.getType() == ConditionLexer.LEADING_DOT_FLOAT) {
                throw new AelParseException("Invalid float literal: " + token.getText());
            }
            // Detect malformed leading-dot floats like "..37": the lexer emits a bare '.' for the
            // first dot (LEADING_DOT_FLOAT requires a digit immediately after '.', so it can't
            // consume it), leaving the parser expecting LEADING_DOT_FLOAT and reporting it in msg.
            if (".".equals(token.getText()) && msg != null && msg.contains("LEADING_DOT_FLOAT")
                    && recognizer instanceof Parser parser) {
                String nextText = parser.getInputStream().LT(2).getText();
                throw new AelParseException("Invalid float literal: " + token.getText() + nextText);
            }
        }
        if (recognizer instanceof Parser) {
            if (firstParserError == null) {
                firstParserError = msg + " at character " + charPositionInLine;
            }
        } else {
            if (firstLexerError == null) {
                firstLexerError = msg + " at character " + charPositionInLine;
            }
        }
    }

    String getErrorMessage() {
        if (firstLexerError != null && firstParserError != null) {
            return "[Lexer] " + firstLexerError + "; [Parser] " + firstParserError;
        }
        if (firstLexerError != null) return "[Lexer] " + firstLexerError;
        if (firstParserError != null) return "[Parser] " + firstParserError;
        return null;
    }
}
