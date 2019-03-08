package com.threathunter.bordercollie.slot.compute.cache.storage;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.LongArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.SecondaryCountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.SecondaryDoubleArrayCacheWrapper;
import com.threathunter.bordercollie.slot.util.HashType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by daisy on 17/3/25.
 */
@Slf4j
public class ByteArrayCacheStorageTest {
    private CacheStore store;
    private List<CacheWrapper> wrappers = new ArrayList<>();

    @Before
    public void setUp() {
        wrappers.add(new LongArrayCacheWrapper.FirstLongArrayCacheWrapper(getCacheWrapperMeta(CacheType.FIRST_LONG)));
        wrappers.add(new SecondaryDoubleArrayCacheWrapper.SecondaryAvgDoubleArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_AVG_DOUBLE)));
        wrappers.add(new SecondaryCountsArrayCacheWrapper.SecondaryDistinctCountArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_DISTINCT_COUNT)));
        store = CacheStoreFactory.newCacheStore(StorageType.BYTES_ARRAY, wrappers);
    }

    @Test
    public void testAllocate() {
        MutableInt totalCacheSize = new MutableInt(0);
        this.wrappers.forEach(wrapper -> totalCacheSize.add(wrapper.getCacheSize()));
        Object obj = store.allocate("key");
        assertThat(obj).isNotNull();
        assertThat(obj).isInstanceOf(byte[].class);
        Assert.assertEquals(totalCacheSize.intValue(), ((byte[]) store.allocate("key")).length);
    }

    @Test
    public void testGetCache() {
        MutableInt totalCacheSize = new MutableInt(0);
        this.wrappers.forEach(wrapper -> totalCacheSize.add(wrapper.getCacheSize()));
        Object obj = store.allocate("key");
        assertThat(obj).isNotNull();
        assertThat(obj).isInstanceOf(byte[].class);
        byte[] bytes = (byte[]) obj;
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (Math.random() * 256);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        log.debug("byte[]={}", bytes);
        Object key = store.getCache("key");

        byte[] getBytes = (byte[]) key;
        assertThat(getBytes).containsExactly(byteBuffer.array());
        log.debug("byte[]={}", getBytes);
    }

    /**
     * Test when all bytes are run out but no out of array index bound
     */
    @Ignore
    @Test
    public void testFullWithData() {
        String key = "key";
        CacheWrapper firstLong = wrappers.get(0);
        CacheWrapper secondaryAvgDouble = wrappers.get(1);
        CacheWrapper secondaryDistinctCount = wrappers.get(2);

        firstLong.addData(1l, key);

        String subKeyPrefix = "sub_";
        for (int i = 0; i < 40; i++) {
            String subKey = subKeyPrefix + i;
            Double avgDouble = (Double) secondaryAvgDouble.addData(1.0, key, subKey);
            Number distinctCount = ((Number) secondaryDistinctCount.addData("geo", key, subKey));
            if (i < 20) {
                Assert.assertEquals(1.0, avgDouble.doubleValue(), 0.0);
                Assert.assertEquals(1, distinctCount.intValue());
            } else {
                Assert.assertNull(avgDouble);
                Assert.assertNull(distinctCount);
            }
        }

        for (int i = 0; i < 40; i++) {
            String subKey = subKeyPrefix + i;
            Double avgDouble = (Double) secondaryAvgDouble.getData(key, subKey);
            Number distinctCount = ((Number) secondaryDistinctCount.getData(key, subKey));
            if (i < 20) {
                Assert.assertEquals(1.0, avgDouble.doubleValue(), 0.0);
                Assert.assertEquals(1, distinctCount.intValue());
            } else {
                Assert.assertNull(avgDouble);
                Assert.assertNull(distinctCount);
            }
        }
    }

    private CacheWrapperMeta getCacheWrapperMeta(CacheType type) {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        meta.setIndexCount(2);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.NORMAL);
        meta.setCacheType(type);

        return meta;
    }

    @Test
    public void testClearAll() {
        store.allocate("key1");
        Object pre = store.getCache("key1");
        assertThat(pre).isNotNull();
        store.clearAll();
        Object current = store.getCache("key1");
        assertThat(current).isNull();
        store.allocate("key2");
        Object o = store.allocate("key3");
        assertThat(o).isNotNull();
    }


    @Test
    public void testGetKeyIterator() {
        String[] strs = new String[]{"key1", "key2", "key3"};
        for (String str : strs) {
            store.allocate(str);
        }
        Iterator<String> keyIterator = store.getKeyIterator();
        int i = 0;
        while (keyIterator.hasNext()) {
            String str = keyIterator.next();
            assertThat(str).isEqualTo(strs[i++]);
            log.debug(str);
        }
    }
}
