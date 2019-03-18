package com.threathunter.bordercollie.slot.util;

import com.threathunter.variable.DimensionType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 
 */
public class DimensionHelper {
    // Here is Sharding base of dimension.
    public static final int DIMENSION_COUNT = 5;

    private static final Map<DimensionType, String> DIMENSION_KEYS;


    static {
        DIMENSION_KEYS = new HashMap<>();
        DIMENSION_KEYS.put(DimensionType.IP, "c_ip");
        DIMENSION_KEYS.put(DimensionType.UID, "uid");
        DIMENSION_KEYS.put(DimensionType.DID, "did");
        DIMENSION_KEYS.put(DimensionType.PAGE, "page");
        DIMENSION_KEYS.put(DimensionType.GLOBAL, "c_ip");
    }

    public static String getDimensionKey(final DimensionType dimensionType) {
        return DIMENSION_KEYS.get(dimensionType);
    }

    public static Set<DimensionType> getAllDimensions() {
        return DIMENSION_KEYS.keySet();
    }
}
