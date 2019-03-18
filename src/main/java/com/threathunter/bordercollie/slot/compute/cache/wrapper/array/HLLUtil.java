package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;

/**
 * 
 */
public class HLLUtil {
    public static String getHLLKey(int wrapperOffset, final String key) {
        return String.format("%d@@%s", wrapperOffset, key).intern();
    }

    public static String getHLLKey(int wrapperOffset, final int hashKey) {
        return String.format("%d@@%d", wrapperOffset, hashKey).intern();
    }

    public static HLL createHLL() {
        return new HLL(9, 5, 0, false, HLLType.FULL);
    }

    public static HLL fromHLL(byte[] target, int offset) {
        byte[] serialize = new byte[fixedSerializedSizeForHLL()];
        System.arraycopy(target, offset, serialize, 0, serialize.length);
        return HLL.fromBytes(serialize);
    }

    public static int fixedSerializedSizeForHLL() {
        return 323;
    }
}
