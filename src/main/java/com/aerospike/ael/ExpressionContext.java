package com.aerospike.ael;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * This class stores input string and optional values for placeholders (if they are used)
 */
@AllArgsConstructor(staticName = "of")
@RequiredArgsConstructor(staticName = "of")
@Getter
public class ExpressionContext {

    /**
     * Input string. If placeholders are used, they should be matched with {@code values}
     */
    private final String expression;
    /**
     * {@link PlaceholderValues} to be matched with placeholders in the {@code input} string.
     * Optional (needed only if there are placeholders)
     */
    private PlaceholderValues values;
}
