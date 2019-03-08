package com.threathunter.bordercollie.slot.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;

import java.util.Arrays;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 1.5
 */
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HashTypePerformanceTest {

    private static String[] tables = new String[100000];

    @Rule
    public TestRule watcher = new TestWatcher() {
        private long start;

        protected void starting(Description description) {
            log.info("Starting test: " + description.getMethodName());
            start = System.currentTimeMillis();
        }

        @Override
        protected void finished(Description description) {
            long end = System.currentTimeMillis();
            log.info("Test " + description.getMethodName() + " took " + (end - start) + "ms");
        }
    };

    @BeforeClass
    public static void setUp() {
        RandomString generator = new RandomString();
        log.info("set up : {}" + HashTypePerformanceTest.class);
        for (int i = 0; i < 100000; i++) {
            tables[i] = generator.nextString();
        }
    }

    @Ignore
    @Test
    public void testaWarmUp() {
        for (int i = 0; i < 1; i++)
            Arrays.stream(tables).forEach(str -> {
                HashType.getHash(str);
                HashType.getMurMurHash(str);
            });
    }

    @Ignore
    @Test
    public void testbNormalHash() {
        for (int i = 0; i < 125; i++)
            Arrays.stream(tables).forEach(str -> {
                HashType.getHash(str);
            });
    }

    @Ignore
    @Test
    public void testcMurmurHash() {
        for (int i = 0; i < 125; i++)
            Arrays.stream(tables).forEach(str -> {
                HashType.getMurMurHash(str);
            });
    }
}
