package com.aerospike.ael.util;

import com.aerospike.ael.client.cdt.CTX;
import com.aerospike.ael.client.exp.Exp;
import com.aerospike.ael.client.exp.ListExp;
import com.aerospike.ael.client.exp.MapExp;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.cdt.list.ListPart;
import com.aerospike.ael.parts.cdt.list.ListTypeDesignator;
import com.aerospike.ael.parts.cdt.map.MapPart;
import com.aerospike.ael.parts.cdt.map.MapTypeDesignator;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.parts.path.BinPart;
import com.aerospike.ael.parts.path.PathFunction;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static com.aerospike.ael.parts.AbstractPart.PartType.LIST_PART;
import static com.aerospike.ael.parts.AbstractPart.PartType.MAP_PART;
import static com.aerospike.ael.parts.cdt.list.ListPart.ListPartType.*;
import static com.aerospike.ael.parts.cdt.map.MapPart.MapPartType.MAP_TYPE_DESIGNATOR;
import static com.aerospike.ael.parts.path.PathFunction.PathFunctionType.*;

@UtilityClass
public class PathOperandUtils {

    /**
     * Processes value type based on the last part of a path and a {@link PathFunction}.
     * If the {@link PathFunction}'s binary type is not null, it's used directly.
     * Otherwise, it checks for list or map type designators, or finds the value type based on the last path part and
     * function type.
     *
     * @param lastPathPart The last {@link AbstractPart} in the path
     * @param pathFunction The {@link PathFunction} associated with the path
     * @return The determined {@link Exp.Type} for the value
     */
    public static Exp.Type processValueType(AbstractPart lastPathPart, PathFunction pathFunction) {
        if (pathFunction.getBinType() == null) {
            if (isListTypeDesignator(lastPathPart)) {
                return Exp.Type.LIST;
            } else if (isMapTypeDesignator(lastPathPart)) {
                return Exp.Type.MAP;
            } else {
                return findValueType(lastPathPart, pathFunction.getPathFunctionType());
            }
        }
        if (pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.CAST) {
            // Value selectors (getByValue/getByValueList/getByValueRange) have no return-type
            // parameter; their constructExp ignores valueType. The cast wrapping is applied
            // afterward in Path.processPath(), so the source-type logic does not apply here.
            if (isValueSelector(lastPathPart)) {
                return TypeUtils.getDefaultType(lastPathPart);
            }
            PathFunction.CastType castType = PathFunction.CastType.valueOf(pathFunction.getBinType().toString());
            return PathFunction.castSourceExpType(castType);
        }
        return Exp.Type.valueOf(pathFunction.getBinType().toString());
    }

    /**
     * Processes and potentially modifies a {@link PathFunction} based on the {@link BasePath} and the last
     * {@link AbstractPart} in the path.
     * This method applies default values for {@link PathFunction} if it's null, or adjusts its type (e.g., from COUNT
     * to SIZE) based on the context of the path parts.
     *
     * @param basePath     The {@link BasePath} containing the bin and CDT parts
     * @param lastPathPart The last {@link AbstractPart} in the path
     * @param pathFunction The {@link PathFunction} to be processed
     * @return The processed {@link PathFunction}
     */
    public static PathFunction processPathFunction(BasePath basePath, AbstractPart lastPathPart,
                                                   PathFunction pathFunction) {
        if (pathFunction == null) {
            // a default path function
            return new PathFunction(GET, PathFunction.ReturnParam.VALUE, null);
        }

        // Use size() operation for non-range CDT count
        PathFunction.ReturnParam defaultReturnParam = PathFunction.ReturnParam.VALUE;
        if (pathFunction.getPathFunctionType() == COUNT) {
            // If there is only a bin or only a CDT designator
            if (basePath.getCdtParts().isEmpty() || containOnlyCdtDesignator(basePath.getCdtParts())) {
                PathFunction.PathFunctionType type = COUNT;
                if (basePath.getBinType() == Exp.Type.LIST || basePath.getBinType() == Exp.Type.MAP) {
                    type = SIZE;
                }
                return new PathFunction(type, defaultReturnParam, null);
            }

            // If the last path part is a CDT type designator, get the previous part
            if (isListTypeDesignator(lastPathPart) || isMapTypeDesignator(lastPathPart)) {
                AbstractPart partBeforeDesignator =
                        getPartOrNull(basePath.getCdtParts(), basePath.getCdtParts().size() - 2);
                if (partBeforeDesignator != null) lastPathPart = partBeforeDesignator;
            }

            // If the last path part is a List index or rank
            if (lastPathPart.getPartType() == LIST_PART) {
                ListPart listPart = (ListPart) lastPathPart;
                if (listPart.getListPartType() == INDEX || listPart.getListPartType() == RANK) {
                    return new PathFunction(SIZE, defaultReturnParam, null);
                }
            }

            // If the last path part is a Map index, rank or key
            if (lastPathPart.getPartType() == MAP_PART) {
                MapPart mapPart = (MapPart) lastPathPart;
                if (mapPart.getMapPartType() == MapPart.MapPartType.INDEX
                        || mapPart.getMapPartType() == MapPart.MapPartType.RANK
                        || mapPart.getMapPartType() == MapPart.MapPartType.KEY) {
                    return new PathFunction(SIZE, defaultReturnParam, null);
                }
            }
        }

        // Apply defaults
        if (pathFunction.getReturnParam() == null) pathFunction =
                new PathFunction(pathFunction.getPathFunctionType(), defaultReturnParam, pathFunction.getBinType());
        if (pathFunction.getPathFunctionType() == null) pathFunction =
                new PathFunction(GET, pathFunction.getReturnParam(), pathFunction.getBinType());

        return pathFunction;
    }

    /**
     * Retrieves an {@link AbstractPart} from a list of parts at a specified index, returning {@code null} if the index
     * is out of bounds.
     *
     * @param parts The list of {@link AbstractPart} objects
     * @param idx   The index of the part to retrieve
     * @return The {@link AbstractPart} at the specified index, or {@code null} if the index is negative
     */
    private static AbstractPart getPartOrNull(List<AbstractPart> parts, int idx) {
        return idx >= 0 ? parts.get(idx) : null;
    }

    /**
     * Checks if a list of {@link AbstractPart} objects contains only a CDT (Collection Data Type) designator.
     * A CDT designator is either a list type designator or a map type designator.
     *
     * @param parts The list of {@link AbstractPart} objects to check
     * @return {@code true} if the list contains only one part which is a CDT designator, {@code false} otherwise
     */
    private static boolean containOnlyCdtDesignator(List<AbstractPart> parts) {
        return parts.size() == 1 && (isListTypeDesignator(parts.get(0)) || isMapTypeDesignator(parts.get(0)));
    }

    /**
     * Checks if the previous CDT (Collection Data Type) part is ambiguous.
     * An ambiguous part is one whose type (e.g., MapPart.MapPartType.INDEX, ListPart.ListPartType.VALUE)
     * could lead to different interpretations without further context.
     *
     * @param lastPart The last {@link AbstractPart} to check for ambiguity
     * @return {@code true} if the last part is an ambiguous CDT part, {@code false} otherwise
     */
    private static boolean isPrevCdtPartAmbiguous(AbstractPart lastPart) {
        if (lastPart.getPartType() == MAP_PART) { // check that lastPart is CDT Map
            // check relevant types
            return List.of(MapPart.MapPartType.INDEX, MapPart.MapPartType.RANK, MapPart.MapPartType.KEY,
                            MapPart.MapPartType.VALUE)
                    .contains(((MapPart) lastPart).getMapPartType());
        }
        if (lastPart.getPartType() == LIST_PART) { // check that lastPart is CDT List
            // check relevant types
            return List.of(INDEX, RANK, ListPart.ListPartType.VALUE)
                    .contains(((ListPart) lastPart).getListPartType());
        }
        return false;
    }

    /**
     * Processes a "get" operation for a path, constructing the appropriate {@link Exp} based on the last path part's
     * type. This method supports {@code LIST_PART} and {@code MAP_PART} types.
     *
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The last {@link AbstractPart} in the path, which determines the type of get operation
     * @param valueType     The expected {@link Exp.Type} of the value being retrieved
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} representing the "get" operation
     * @throws UnsupportedOperationException If the path part type is not supported
     */
    public static Exp processGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {
        if (lastPathPart.getPartType() != LIST_PART && lastPathPart.getPartType() != MAP_PART) {
            throw new UnsupportedOperationException(
                    String.format("Path part type %s is not supported", lastPathPart.getPartType()));
        }
        return doProcessCdtGet(basePath, lastPathPart, valueType, cdtReturnType);
    }

    /**
     * Helper method to process CDT (Collection Data Type) "get" operations.
     * It constructs the appropriate {@link Exp} for list or map type designators,
     * or for specific CDT parts with their context.
     *
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The last {@link AbstractPart} in the path
     * @param valueType     The expected {@link Exp.Type} of the value being retrieved
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} representing the CDT "get" operation
     */
    private static Exp doProcessCdtGet(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                       int cdtReturnType) {
        // list type designator "[]" can be either after bin name or after path
        if (isListTypeDesignator(lastPathPart) || isMapTypeDesignator(lastPathPart)) {
            return constructCdtExp(basePath, lastPathPart, valueType, cdtReturnType);
        }

        // Context can be empty
        CTX[] context = getContextArray(basePath.getCdtParts(), false);
        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
    }

    /**
     * Checks if an {@link AbstractPart} is a list type designator (e.g., "[]").
     *
     * @param cdtPart The {@link AbstractPart} to check
     * @return {@code true} if the part is a list type designator, {@code false} otherwise
     */
    private static boolean isListTypeDesignator(AbstractPart cdtPart) {
        return cdtPart.getPartType() == LIST_PART
                && ((ListPart) cdtPart).getListPartType().equals(LIST_TYPE_DESIGNATOR);
    }

    /**
     * Checks if an {@link AbstractPart} is a map type designator (e.g., "{}").
     *
     * @param cdtPart The {@link AbstractPart} to check
     * @return {@code true} if the part is a map type designator, {@code false} otherwise
     */
    private static boolean isMapTypeDesignator(AbstractPart cdtPart) {
        return cdtPart.getPartType() == MAP_PART && ((MapPart) cdtPart).getMapPartType().equals(MAP_TYPE_DESIGNATOR);
    }

    private static boolean isValueSelector(AbstractPart cdtPart) {
        if (cdtPart.getPartType() == LIST_PART) {
            ListPart.ListPartType type = ((ListPart) cdtPart).getListPartType();
            return type == ListPart.ListPartType.VALUE || type == ListPart.ListPartType.VALUE_LIST
                    || type == ListPart.ListPartType.VALUE_RANGE;
        }
        if (cdtPart.getPartType() == MAP_PART) {
            MapPart.MapPartType type = ((MapPart) cdtPart).getMapPartType();
            return type == MapPart.MapPartType.VALUE || type == MapPart.MapPartType.VALUE_LIST
                    || type == MapPart.MapPartType.VALUE_RANGE;
        }
        return false;
    }

    /**
     * Constructs an array of {@link CTX} (context) objects from a list of {@link AbstractPart} objects.
     * This is used to build the context for nested CDT operations.
     *
     * @param parts       The list of {@link AbstractPart} objects representing path parts
     * @param includesLast A boolean indicating whether the last part should be included in the context array
     * @return An array of {@link CTX} objects
     */
    public static CTX[] getContextArray(List<AbstractPart> parts, boolean includesLast) {
        // Nested (Context) map key access
        List<CTX> context = new ArrayList<>();

        for (int i = 0; i < parts.size(); i++) {
            if (!includesLast && i == parts.size() - 1) {
                // Skip last
                continue;
            }
            AbstractPart part = parts.get(i);
            context.add(((CdtPart) part).getContext());
        }
        return context.toArray(new CTX[0]);
    }

    /**
     * Processes a "size" operation for a path, constructing the appropriate {@link Exp} based on the last path part's type.
     * This method supports {@code LIST_PART} and {@code MAP_PART} types.
     *
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The last {@link AbstractPart} in the path, which determines the type of size operation
     * @param valueType     The expected {@link Exp.Type} of the value whose size is being retrieved
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} representing the "size" operation
     * @throws UnsupportedOperationException If the path part type is not supported
     */
    public static Exp processSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType, int cdtReturnType) {
        if (lastPathPart.getPartType() == LIST_PART) {
            return processListPartSize(basePath, lastPathPart, valueType, cdtReturnType);
        } else if (lastPathPart.getPartType() == MAP_PART) {
            return processMapPartSize(basePath, lastPathPart, valueType, cdtReturnType);
        }
        throw new UnsupportedOperationException(
                String.format("Path part type %s is not supported", lastPathPart.getPartType()));
    }

    /**
     * Processes the "size" operation for a {@link ListPart}.
     * It handles list type designators and constructs the appropriate {@link ListExp#size} expression.
     *
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The {@link ListPart} representing the last part of the path
     * @param valueType     The expected {@link Exp.Type} of the list elements
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} representing the list size operation
     */
    private static Exp processListPartSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                           int cdtReturnType) {
        BinPart bin = basePath.getBinPart();
        ListPart listPart = (ListPart) lastPathPart;

        // list type designator "[]" can be either after bin name or after path
        if (listPart.getListPartType().equals(LIST_TYPE_DESIGNATOR)) {
            return getCdtExpFunction(ListExp::size, basePath, lastPathPart, valueType, cdtReturnType);
        }
        // In size() the last element is considered context
        CTX[] context = getContextArray(basePath.getCdtParts(), true);
        return ListExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
    }

    /**
     * Processes the "size" operation for a {@link MapPart}.
     * It handles map type designators and constructs the appropriate {@link MapExp#size} expression.
     *
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The {@link MapPart} representing the last part of the path
     * @param valueType     The expected {@link Exp.Type} of the map elements
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} representing the map size operation
     */
    private static Exp processMapPartSize(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                          int cdtReturnType) {
        BinPart bin = basePath.getBinPart();
        MapPart mapPart = (MapPart) lastPathPart;

        if (mapPart.getMapPartType().equals(MAP_TYPE_DESIGNATOR)) {
            return getCdtExpFunction(MapExp::size, basePath, lastPathPart, valueType, cdtReturnType);
        }
        // In size() the last element is considered context
        CTX[] context = getContextArray(basePath.getCdtParts(), true);
        return MapExp.size(Exp.bin(bin.getBinName(), basePath.getBinType()), context);
    }

    /**
     * Updates the {@link BasePath} with a CDT (Collection Data Type) type designator if necessary.
     * This is typically done for operations like {@code list.count()} or {@code map.count()} when no explicit
     * designator (e.g., {@code []} for List, {@code {}} for Map) is present, and the last CDT part is ambiguous.
     *
     * @param basePath     The {@link BasePath} to be updated
     * @param pathFunction The {@link PathFunction} associated with the operation
     */
    public static void updateWithCdtTypeDesignator(BasePath basePath, PathFunction pathFunction) {
        if (mustHaveCdtDesignator(pathFunction, basePath.getCdtParts())) {
            // For cases like list.count() and map.count() with no explicit designator ([] is for List, {} is for Map)
            // When the last path part is CDT and potentially ambiguous, we apply List designator by default
            AbstractPart lastPathPart = new ListTypeDesignator();
            basePath.getCdtParts().add(lastPathPart);
        }
    }

    /**
     * Determines if a CDT (Collection Data Type) designator must be present based on the {@link PathFunction}
     * and the list of {@link AbstractPart} objects.
     * A designator is required if the function type is {@code SIZE} or {@code COUNT}, or if it's a {@code GET} with
     * {@code COUNT} return parameter, and the parts are empty or the previous CDT part is ambiguous.
     *
     * @param pathFunction The {@link PathFunction} to evaluate
     * @param parts        The list of {@link AbstractPart} objects representing the path
     * @return {@code true} if a CDT designator is mandatory, {@code false} otherwise
     */
    private static boolean mustHaveCdtDesignator(PathFunction pathFunction,
                                                 List<AbstractPart> parts) {
        // if existing path function type is SIZE or COUNT
        // and parts are empty (only bin) or previous CDT part is ambiguous (CDT index, rank or map key)
        if (pathFunction == null) return false;
        PathFunction.PathFunctionType type = pathFunction.getPathFunctionType();
        return (List.of(SIZE, COUNT).contains(type) || pathFunctionIsGetWithCount(pathFunction))
                && (parts.isEmpty() || (isPrevCdtPartAmbiguous(parts.get(parts.size() - 1))));
    }

    /**
     * Checks if the given {@link PathFunction} represents a "GET" operation with a "COUNT" return parameter.
     *
     * @param pathFunction The {@link PathFunction} to check
     * @return {@code true} if it's a GET with COUNT return parameter, {@code false} otherwise.\
     */
    private static boolean pathFunctionIsGetWithCount(PathFunction pathFunction) {
        return pathFunction.getPathFunctionType() == GET
                && pathFunction.getReturnParam() == PathFunction.ReturnParam.COUNT;
    }

    /**
     * Constructs a CDT (Collection Data Type) expression.
     * This method builds the context array from the path parts (excluding the last one if it's a designator)
     * and then uses the last path part to construct the final expression.
     *
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The last {@link AbstractPart} in the path
     * @param valueType     The expected {@link Exp.Type} of the value
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} representing the constructed CDT expression
     */
    private static Exp constructCdtExp(BasePath basePath, AbstractPart lastPathPart, Exp.Type valueType,
                                       int cdtReturnType) {
        // Context can be empty
        List<AbstractPart> partsUpToDesignator = basePath.getCdtParts().isEmpty()
                ? new ArrayList<>()
                : basePath.getCdtParts().subList(0, basePath.getCdtParts().size() - 1);
        CTX[] context = getContextArray(partsUpToDesignator, false);

        return ((CdtPart) lastPathPart).constructExp(basePath, valueType, cdtReturnType, context);
    }

    /**
     * Applies a unary operator to a constructed CDT (Collection Data Type) expression.
     * This is a generic method to apply functions like {@code size()} to CDT expressions.
     *
     * @param operator      The {@link UnaryOperator} to apply (e.g., {@code ListExp::size}, {@code MapExp::size})
     * @param basePath      The {@link BasePath} of the expression
     * @param lastPathPart  The last {@link AbstractPart} in the path
     * @param valueType     The expected {@link Exp.Type} of the value
     * @param cdtReturnType The CDT return type
     * @return An {@link Exp} resulting from applying the operator to the CDT expression
     */
    private static Exp getCdtExpFunction(UnaryOperator<Exp> operator, BasePath basePath, AbstractPart lastPathPart,
                                         Exp.Type valueType, int cdtReturnType) {
        Exp cdtExp = constructCdtExp(basePath, lastPathPart, valueType, cdtReturnType);
        return operator.apply(cdtExp);
    }

    /**
     * Determines the value type based on the last path part and the path function type.
     * It prioritizes the explicit type of the last path part, then checks for count-related types,
     * and finally falls back to a default type.
     *
     * @param lastPathPart     The last {@link AbstractPart} in the path
     * @param pathFunctionType The {@link PathFunction.PathFunctionType} of the operation
     * @return The determined {@link Exp.Type} for the value
     */
    private static Exp.Type findValueType(AbstractPart lastPathPart, PathFunction.PathFunctionType pathFunctionType) {
        /*
            Determine valueType based on
            1. The last path part
            2. Path function type
            3. Default type
         */
        if (lastPathPart != null && lastPathPart.getExpType() != null) {
            return lastPathPart.getExpType();
        } else if (pathFunctionType == COUNT) {
            return getValueTypeForCount(lastPathPart);
        }
        return lastPathPart != null ? TypeUtils.getDefaultType(lastPathPart) : Exp.Type.INT;
    }

    /**
     * Determines the value type specifically for a "COUNT" operation based on the last path part.
     * If the last path part is a {@link ListTypeDesignator}, it returns {@code Exp.Type.LIST}.
     * If it's a {@link MapTypeDesignator}, it returns {@code Exp.Type.MAP}.
     * Otherwise, it returns the default type for count operations.
     *
     * @param lastPathPart The last {@link AbstractPart} in the path
     * @return The {@link Exp.Type} appropriate for a count operation
     */
    private static Exp.Type getValueTypeForCount(AbstractPart lastPathPart) {
        if (lastPathPart instanceof ListTypeDesignator) {
            return Exp.Type.LIST;
        } else if (lastPathPart instanceof MapTypeDesignator) {
            return Exp.Type.MAP;
        }
        return TypeUtils.getDefaultTypeForCount();
    }
}
