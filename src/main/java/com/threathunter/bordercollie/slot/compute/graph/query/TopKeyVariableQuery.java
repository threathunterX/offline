package com.threathunter.bordercollie.slot.compute.graph.query;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17/4/3.
 */
public class TopKeyVariableQuery implements VariableQuery, TopQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopKeyVariableQuery.class);
    private final int count;
    private final int copy;
    private final CountDownLatch latch;
    private final BlockingQueue<Object> results;

    public TopKeyVariableQuery(int copy, int topCount) {
        this.copy = copy;
        this.count = topCount;
        this.latch = new CountDownLatch(copy);
        this.results = new ArrayBlockingQueue<>(copy);
    }

    @Override
    public Object waitQueryResult(int timeout, TimeUnit unit) {
        try {
            boolean finish = latch.await(timeout, unit);
            if (!finish) {
                LOGGER.error(String.format("require for %d copies, but we get only %d", this.copy, this.results.size()));
            }
        } catch (Exception e) {
            // can add variable id
            LOGGER.error("esper:interrupted during waiting for the results on global query ", e);
        }
        List<Map<String, Object>> list = new ArrayList<>();
        this.results.forEach(r -> {
            List<Map<String, Object>> tops = (List<Map<String, Object>>) r;
            if (tops != null) {
                list.addAll(tops);
            }
        });
        Collections.sort(list, (m1, m2) -> ((Number) m2.get("value")).intValue() - ((Number) m1.get("value")).intValue());

        Map<String, Object> data = new HashMap<>();
        if (list.size() <= count) {
            data.put(CacheConstants.GLOBAL_KEY, list);
        } else {
            data.put(CacheConstants.GLOBAL_KEY, list.subList(0, count));
        }

        return data;
    }

    @Override
    public void addResult(final Object result) {
        if (result != null) {
            this.results.add(result);
        }
        this.latch.countDown();
    }

    @Override
    public int getTopCount() {
        return count;
    }
}
