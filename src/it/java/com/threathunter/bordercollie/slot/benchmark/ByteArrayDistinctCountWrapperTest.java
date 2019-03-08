package com.threathunter.bordercollie.slot.benchmark;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStoreFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 2.16
 */
@Slf4j
public class ByteArrayDistinctCountWrapperTest {
    public static CacheStore store;
    public static CacheWrapper cacheWrapper;
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
    String[] values_16D = new String[]{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10", "value11", "value12", "value13", "value14", "value15", "value16"};
    String[] values_40D = new String[]{"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10", "value11", "value12", "value13", "value14", "value15", "value16",
                                        "value17", "value18", "value19", "value20", "value21", "value22", "value23", "value24", "value25", "value26", "value27", "value28", "value29", "value30", "value31",
                                        "value32","value33", "value34", "value35", "value36", "value37", "value38", "value7", "value39", "value40"};

    @BeforeClass
    public static void setup() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.DISTINCT_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        cacheWrapper = CacheWrapperFactory.createCacheWrapper(meta);
        List<CacheWrapper> wrapperList = new ArrayList<CacheWrapper>();
        wrapperList.add(cacheWrapper);
        store = CacheStoreFactory.newCacheStore(StorageType.BYTES_ARRAY, wrapperList);
    }

    /**
     * 10000次
     * 一组测试 16个唯一值，40个唯一值
     * Test testDistinctCount_16DistinctItem took 27ms
     * Test testDistinctCount_40DistinctItem took 270ms
     */
    @Test
    public void testDistinctCount_16DistinctItem() {
        Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            int index = random.nextInt(16);
            cacheWrapper.addData(this.values_16D[index], "key");
        }

    }

    @Test
    public void testDistinctCount_40DistinctItem() {
        Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            int index = random.nextInt(40);
            cacheWrapper.addData(this.values_40D[index], "key");
        }

    }



    /**
     * 100000次
     * 一组测试 16个唯一值，40个唯一值
     * Test testDistinctCount_16DistinctItem took 39ms
     * Test testDistinctCount_40DistinctItem took 829ms
     *
     * 10000000次
     * 一组测试 16个唯一值，40个唯一值
     *      * Test testDistinctCount_16DistinctItem took 335ms
     * Test testDistinctCount_40DistinctItem took 16326ms
     *
     */
    @Test
    public void testDistinctCount_10WRound_16DistinctItem() {
        Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            int index = random.nextInt(16);
            cacheWrapper.addData(this.values_16D[index], "key");
        }

    }

    @Test
    public void testDistinctCount_10WRound_40DistinctItem() {
        Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            int index = random.nextInt(40);
            cacheWrapper.addData(this.values_40D[index], "key");
        }

    }


    /**
     * 以本地slot_metas.json为例，distinct 变量共有29 个，假设以每秒2000的event吞吐量来算
     * distinct count  总共计算了2000 * 29 次。
     * 我们假设两种情况：
     * 最好情况：都走16Distinct 的值，Test testGoodSituation took 44ms
     * 最坏情况：都走40Distinct 的值。Test testBadSituation took 530ms
     */

    @Test
    public void testGoodSituation(){
        int total=2000*29;
        Random random = new Random();
        for (int i = 0; i < total; i++) {
            int index = random.nextInt(16);
            cacheWrapper.addData(this.values_16D[index], "key");
        }
    }

    @Test
    public void testBadSituation(){
        int total=2000*29;
        Random random = new Random();
        for (int i = 0; i < total; i++) {
            int index = random.nextInt(40);
            cacheWrapper.addData(this.values_40D[index], "key");
        }
    }
}
