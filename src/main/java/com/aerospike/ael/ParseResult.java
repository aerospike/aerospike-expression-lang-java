package com.aerospike.ael;

import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.query.Filter;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * This class stores result of parsing AEL expression using {@link ParsedExpression#getResult()}
 * in form of Java client's secondary index {@link Filter} and filter {@link Exp}.
 */
@AllArgsConstructor
@Getter
public class ParseResult {

    /**
     * Secondary index {@link Filter}. Can be null in case of invalid or unsupported AEL string
     */
    Filter filter;
    /**
     * Filter {@link Exp}. Can be null in case of invalid or unsupported AEL string
     */
    Exp exp;
}
