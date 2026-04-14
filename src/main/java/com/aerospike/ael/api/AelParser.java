package com.aerospike.ael.api;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.query.Filter;

/**
 * Contains API to convert dot separated String path into an Aerospike filter -
 * a functional language for applying predicates to bin data and record metadata.
 * <br>
 * Such filters are used in different areas of Aerospike Server functionality including the following:
 * <ul>
 * <li> filtering queries (acting as the WHERE clause),</li>
 * <li> filtering batch operations,</li>
 * <li> conditionally executing single key operations (get, put, delete, operate),</li>
 * <li> defining secondary indexes.</li>
 * </ul>
 */
public interface AelParser {

    /**
     * Parse AEL string into {@link ParsedExpression}.
     * <br><br>
     * Examples:
     * <table border="1">
     * <caption>Path element</caption>
     *   <tr>
     *     <td> $.binName </td> <td> Bin “binName” </td>
     *   </tr>
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> '1' </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {='1'} </td>
     *     <td> Map value (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> {#1} </td>
     *     <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     *   </tr>
     * </table>
     * <br>
     * <table border="1">
     * <caption>A nested element</caption>
     *   <tr>
     *     <td> $.mapBinName.k </td> <td> [mapBinName -> mapKey("a")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.aa.aaa </td> <td> [mapBinName -> mapKey("a") -> mapKey("aa") -> mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.55 </td> <td> [mapBinName -> mapKey("a") -> mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> $.listBinName.[1].aa </td> <td> [listBinName -> listIndex(1) -> mapKey("aa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.ab.cd.[-1].'10' </td> <td> [mapBinName -> mapKey("ab") -> mapKey("cd") -> listIndex(-1) ->
     *         mapKey("10")] </td>
     *   </tr>
     * </table>
     *
     * @param input {@link ExpressionContext} containing input string of dot separated elements. If the input string has
     *              placeholders, matching values must be provided within {@code input} too
     * @return {@link ParsedExpression} object
     * @throws AelParseException in case of invalid syntax
     */
    ParsedExpression parseExpression(ExpressionContext input);

    /**
     * Parse AEL string into {@link ParsedExpression}.
     * <br><br>
     * Examples:
     * <table border="1">
     * <caption>Path element</caption>
     *   <tr>
     *     <td> $.binName </td> <td> Bin “binName” </td>
     *   </tr>
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> '1' </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {='1'} </td>
     *     <td> Map value (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> {#1} </td>
     *     <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     *   </tr>
     * </table>
     * <br>
     * <table border="1">
     * <caption>A nested element</caption>
     *   <tr>
     *     <td> $.mapBinName.k </td> <td> [mapBinName -> mapKey("a")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.aa.aaa </td> <td> [mapBinName -> mapKey("a") -> mapKey("aa") -> mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.a.55 </td> <td> [mapBinName -> mapKey("a") -> mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> $.listBinName.[1].aa </td> <td> [listBinName -> listIndex(1) -> mapKey("aa")] </td>
     *   </tr>
     *   <tr>
     *     <td> $.mapBinName.ab.cd.[-1].'10' </td> <td> [mapBinName -> mapKey("ab") -> mapKey("cd") -> listIndex(-1) ->
     *         mapKey("10")] </td>
     *   </tr>
     * </table>
     *
     * @param input        {@link ExpressionContext} containing input string of dot separated elements. If the input string has
     *                     placeholders, matching values must be provided within {@code input} too
     * @param indexContext Class containing namespace and collection of {@link Index} objects that represent
     *                     existing secondary indexes (optionally including {@link Index#getSetName()} and
     *                     {@link IndexContext#getQuerySet()} aligned with {@code Statement.setSetName}).
     *                     Required for creating {@link Filter}. Can be null
     * @return {@link ParsedExpression} object
     * @throws AelParseException in case of invalid syntax
     */
    ParsedExpression parseExpression(ExpressionContext input, IndexContext indexContext);

    /**
     * Parse AEL path with CDT context into an array of {@link CTX} objects. The argument must represent a path with context,
     * e.g. $.listBinName.[1], $.mapBinName.ab etc.
     *
     * @param aelPath Input string representing path with CDT context, must not be null
     * @return Array of {@link CTX}
     * @throws AelParseException in case of invalid syntax
     */
    CTX[] parseCTX(String aelPath);
}
