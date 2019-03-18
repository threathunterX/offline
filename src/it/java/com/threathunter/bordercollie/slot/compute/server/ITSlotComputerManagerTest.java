package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.bordercollie.slot.compute.SlotComputeManager;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 */
public class ITSlotComputerManagerTest extends ITCommonBase {
    private static SlotComputeManager slotComputingManager;


    @Ignore
    @Test
    public void testContainsKeyBuiltin() throws InterruptedException {
        slotComputingManager = new SlotComputeManager(dimensionTypes, StorageType.BUILDIN, false);
        slotComputingManager.start();
    }


}
