package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ByteArrayCacheStore.class)
public class DoubleArrayCacheWrapperMergeTest {
    private Map<String, byte[]> data = new HashMap<>();
    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private static final String TEST_KEY = "TEST";
    private static final String TEST_KEY_1 = "TEST_1";

    @Test
    public void testDoubleStddevMerge() {
        initialCacheSize(20, TEST_KEY);
        initialCacheSize(20, TEST_KEY_1);

        DoubleArrayCacheWrapper.StddevDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.StddevDoubleArrayCacheWrapper(getCacheWrapperMeta());
        wrapper.updateStoreInfo(cacheStore, 0);
        PrimaryData merged = wrapper.merge(null, TEST_KEY);
        Assert.assertNull(merged);

        wrapper.addData(1.0, TEST_KEY);
        wrapper.addData(1.0, TEST_KEY);
        wrapper.addData(2.0, TEST_KEY);
        wrapper.addData(5.0, TEST_KEY);
        Assert.assertEquals(3.58333, wrapper.getData(TEST_KEY), 0.00001);

        PrimaryData first = wrapper.merge(merged, TEST_KEY);
        Assert.assertEquals(3.58333, ((Number) first.getResult()).doubleValue(), 0.00001);

        wrapper.addData(2.0, TEST_KEY_1);
        wrapper.addData(0.0, TEST_KEY_1);
        wrapper.addData(10.0, TEST_KEY_1);
        Assert.assertEquals(28, wrapper.getData(TEST_KEY_1), 0.00001);

        PrimaryData second = wrapper.merge(first, TEST_KEY_1);
        Assert.assertEquals(12, ((Number) second.getResult()).doubleValue(), 0.00001);
    }

    @Test
    public void testDoubleCVMerge() {

    }

    private void initialCacheSize(int size, String testKey) {
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(testKey)).when(cacheStore).getCache(testKey);
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(testKey, new byte[size]);
            return data.get(testKey);
        }).when(cacheStore).allocate(testKey);
    }

    private CacheWrapperMeta getCacheWrapperMeta() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.STDDEV_DOUBLE);

        return meta;
    }
}
