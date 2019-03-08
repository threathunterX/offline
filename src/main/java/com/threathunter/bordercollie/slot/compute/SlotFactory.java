package com.threathunter.bordercollie.slot.compute;


import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 17-11-7.
 */
public class SlotFactory {
    public static SlotEngine createSlotEngine(int interval, TimeUnit timeUnit, Set<DimensionType> enableDimensions, List<VariableMeta> metas) {
        return new SlotEngine(interval, timeUnit, enableDimensions, metas, StorageType.BYTES_ARRAY);
    }

    public static SlotEngine createSlotEngine(int interval, TimeUnit timeUnit, Set<DimensionType> enableDimensions, List<VariableMeta> metas, StorageType type) {
        return new SlotEngine(interval, timeUnit, enableDimensions, metas, type);
    }

    public static SlotEngine createSlotEngine(Set<DimensionType> enableDimensions, List<VariableMeta> metas) {
        return SlotFactory.createSlotEngine(5, TimeUnit.MINUTES, enableDimensions, metas);
    }

    public static SlotWindow createSlotWindow(Long id) {
        return new SlotWindow(id);
    }

    public static SlotQueryable createSlotQueryable(SlotEngine engine) {
        return new SlotQuery(engine);
    }

}
