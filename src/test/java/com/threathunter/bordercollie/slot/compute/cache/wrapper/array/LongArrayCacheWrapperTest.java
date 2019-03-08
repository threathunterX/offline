package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by toyld on 3/21/17.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ByteArrayCacheStore.class)
public class LongArrayCacheWrapperTest {
    private Map<String, byte[]> data = new HashMap<>();
    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private static final String TEST_KEY = "TEST";

    @Before
    public void setUp() {
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(TEST_KEY)).when(cacheStore).getCache(TEST_KEY);

        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(TEST_KEY, new byte[84]);
            return data.get(TEST_KEY);
        }).when(cacheStore).allocate(TEST_KEY);
    }

    @Test
    public void testSumLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SUM_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);

        LongArrayCacheWrapper.SumLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.SumLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 10);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(1l, wrapper.addData(1l, TEST_KEY).longValue());
        Assert.assertEquals(1l, wrapper.getData(TEST_KEY).longValue());
        Assert.assertEquals(0, wrapper.addData(-1l, TEST_KEY).longValue());
    }

    @Test
    public void testFirstLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.FIRST_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);

        LongArrayCacheWrapper.FirstLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.FirstLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 10);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0L, wrapper.addData(0L, TEST_KEY).longValue());
        // return null if already add the first value
        Assert.assertNull(wrapper.addData(1L, TEST_KEY));
        Assert.assertEquals(0L, wrapper.getData(TEST_KEY).longValue());
    }

    @Test
    public void testLastLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.LAST_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);

        LongArrayCacheWrapper.LastLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.LastLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0L, wrapper.addData(0L, TEST_KEY).longValue());


        Assert.assertEquals(Long.MAX_VALUE, wrapper.addData(Long.MAX_VALUE, TEST_KEY).longValue());
        Assert.assertEquals(Long.MIN_VALUE, wrapper.addData(Long.MIN_VALUE, TEST_KEY).longValue());
        Assert.assertEquals(Long.MIN_VALUE, wrapper.getData(TEST_KEY).longValue());
    }

    @Test
    public void testMaxLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MAX_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);

        LongArrayCacheWrapper.MaxLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.MaxLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0, wrapper.addData(0L, TEST_KEY).longValue());
        Assert.assertEquals(2L, wrapper.addData(2L, TEST_KEY).longValue());
        Assert.assertEquals(Long.MAX_VALUE, wrapper.addData(Long.MAX_VALUE, TEST_KEY).longValue());
        Assert.assertNull(wrapper.addData(Long.MIN_VALUE, TEST_KEY));

        Assert.assertEquals(Long.MAX_VALUE, wrapper.getData(TEST_KEY).longValue());
    }

    @Test
    public void testMinLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MIN_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);

        LongArrayCacheWrapper.MinLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.MinLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0L, wrapper.addData(0L, TEST_KEY).longValue()); // need Attetion, First value is 0, no insert.
        Assert.assertNull(wrapper.addData(1L, TEST_KEY));
        Assert.assertEquals(Long.MIN_VALUE, wrapper.addData(Long.MIN_VALUE, TEST_KEY).longValue());
        Assert.assertNull(wrapper.addData(2L, TEST_KEY));
        Assert.assertEquals(Long.MIN_VALUE, wrapper.getData(TEST_KEY).longValue());
    }

    @Test
    public void testAvgLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.AVG_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);

        LongArrayCacheWrapper.AvgLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.AvgLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(3, wrapper.addData(3l, TEST_KEY).intValue());
        Assert.assertEquals(4, wrapper.addData(5l, TEST_KEY).intValue());
        Assert.assertEquals(4, wrapper.getData(TEST_KEY).intValue());
    }

    @Test
    public void testStddevLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.STDDEV_LONG);

        LongArrayCacheWrapper.StddevLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.StddevLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertNull(wrapper.getData(TEST_KEY));
        Assert.assertEquals(0.0, wrapper.addData(1l, TEST_KEY), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(1l, TEST_KEY), 0.0);
        Assert.assertEquals(0.3333, wrapper.addData(2l, TEST_KEY), 0.0001);
        Assert.assertEquals(3.58333, wrapper.addData(5l, TEST_KEY), 0.00001);

        Assert.assertEquals(3.58333, wrapper.getData(TEST_KEY), 0.00001);
    }

    @Test
    public void testCVLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.CV_LONG);

        LongArrayCacheWrapper.CVLongArrayCacheWrapper wrapper = new LongArrayCacheWrapper.CVLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertNull(wrapper.getData(TEST_KEY));
        Assert.assertEquals(0.0, wrapper.addData(1l, TEST_KEY), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(1l, TEST_KEY), 0.0);
        Assert.assertEquals(0.43301, wrapper.addData(2l, TEST_KEY), 0.00001);
        Assert.assertEquals(0.84132, wrapper.addData(5l, TEST_KEY), 0.00001);

        Assert.assertEquals(0.84132, wrapper.getData(TEST_KEY), 0.00001);
    }
}
