package com.threathunter.bordercollie.slot.compute.cache.storage;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin.GlobalCountsBuiltinCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin.SecondaryDoubleBuiltinCacheWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by daisy on 17/3/21.
 */
public class BuiltinCacheStorageTest {
    private BuiltinCacheStore store;
    private List<CacheWrapper> wrappers = new ArrayList<>();

    @Before
    public void setUp() {
        CacheWrapperMeta globalMeta = new CacheWrapperMeta();
        globalMeta.setCacheType(CacheType.GLOBAL_COUNT);
        wrappers.add(new GlobalCountsBuiltinCacheWrapper.GlobalCountBuiltinCacheWrapper(globalMeta));

        CacheWrapperMeta secondaryMeta = new CacheWrapperMeta();
        secondaryMeta.setCacheType(CacheType.SECONDARY_AVG_DOUBLE);
        wrappers.add(new SecondaryDoubleBuiltinCacheWrapper.SecondaryAvgDoubleBuiltinCacheWrapper(secondaryMeta));
        store = new BuiltinCacheStore(wrappers);
    }

    @Test
    public void testAllocate() {
        //secondary_avg_double cache size is 2
        Assert.assertEquals(3, store.allocate("key").length);
        Assert.assertEquals(3, store.getCache("key").length);
    }

    @Test
    public void testWrapperStorage() {
        Assert.assertEquals(1, wrappers.get(0).addData(null, null));
        Assert.assertEquals(2, wrappers.get(0).addData(null, null));
        Assert.assertEquals(2, wrappers.get(0).getData(null));

        Assert.assertEquals(1.0, wrappers.get(1).addData(1.0, "key", "key1"));
        Assert.assertEquals(0.5, wrappers.get(1).addData(0.0, "key", "key1"));
        Assert.assertEquals(0.5, wrappers.get(1).getData("key", "key1"));
    }
}
