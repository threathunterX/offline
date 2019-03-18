package com.threathunter.bordercollie.slot.compute.graph.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 
 */
public class KeyValueTopVariableQuery implements VariableQuery, TopQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueTopVariableQuery.class);
    private final BlockingQueue<Object> results;
    private final CountDownLatch latch;
    private final int shardCount;
    private final int topCount;

    public KeyValueTopVariableQuery(int topCount) {
        this(1, topCount);
    }

    public KeyValueTopVariableQuery(int shardCount, int topCount) {
        this.results = new ArrayBlockingQueue(shardCount);
        this.latch = new CountDownLatch(shardCount);
        this.shardCount = shardCount;
        this.topCount = topCount;
    }

    @Override
    public Object waitQueryResult(int timeout, final TimeUnit unit) {
        try {
            boolean finish = latch.await(timeout, unit);
            if (!finish) {
                LOGGER.error(String.format("require for %d copies, but we get only %d", this.shardCount, this.results.size()));
            }
        } catch (Exception e) {
            LOGGER.error("esper:interrupted during waiting for the results on global query ", e);
        }
        if (shardCount == 1) {
            try {
                return this.results.poll(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                return null;
            }
        }
        Map<String, Object> data = new HashMap<>();
        this.results.forEach(d -> data.putAll((Map) d));
        return data;
    }

    @Override
    public void addResult(final Object result) {
        try {
            if (result != null) {
                this.results.add(result);
            }
            this.latch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getTopCount() {
        return topCount;
    }
}
