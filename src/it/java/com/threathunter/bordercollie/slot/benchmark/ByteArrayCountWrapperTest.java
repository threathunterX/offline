package com.threathunter.bordercollie.slot.benchmark;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStoreFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin.CountsBuiltinCacheWrapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 2.16
 */
@Slf4j
public class ByteArrayCountWrapperTest {
    public static CacheStore store;
    public static CacheWrapper cacheWrapper;
    @BeforeClass
    public static void setup(){
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        cacheWrapper = CacheWrapperFactory.createCacheWrapper(meta);
        List<CacheWrapper> wrapperList=new  ArrayList<CacheWrapper>();
        wrapperList.add(cacheWrapper);
        store=CacheStoreFactory.newCacheStore(StorageType.BYTES_ARRAY,wrapperList);
    }

    @Rule
    public TestRule watcher = new TestWatcher() {
        private long start;

        protected void starting(Description description) {
            log.info("===================================");
            log.info("Starting test: " + description.getMethodName());
            start = System.currentTimeMillis();
        }

        @Override
        protected void finished(Description description) {
            long end = System.currentTimeMillis();
            log.info("Test " + description.getMethodName() + " took " + (end - start) + "ms");
            log.info("===================================");
        }
    };

    /**
     * Starting test: test1WRound_Count
     *
     *  Test test1WRound_Count took 11ms
     */
    @Test
    public void test1WRound_Count(){
        for(int i=0;i<10000;i++)
       cacheWrapper.addData("","key1");
    }



}
