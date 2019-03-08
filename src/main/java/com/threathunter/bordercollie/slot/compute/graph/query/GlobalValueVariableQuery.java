package com.threathunter.bordercollie.slot.compute.graph.query;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants;
import com.threathunter.bordercollie.slot.util.ConstantsUtil;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17/4/3.
 */
public class GlobalValueVariableQuery implements VariableQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalValueVariableQuery.class);
    private final CountDownLatch latch;
    private final int copy;
    private final BlockingQueue<Object> results;

    public GlobalValueVariableQuery(final int copy) {
        this.copy = copy;
        this.latch = new CountDownLatch(copy);
        this.results = new ArrayBlockingQueue(copy);
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
        // count or distinct count
        MutableLong count = new MutableLong(0);
        for (Object obj : results) {
            // key is timestamp while value is the query data
            if (obj instanceof Number) {
                count.add((Number) obj);
            }
        }


        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put(CacheConstants.GLOBAL_KEY, count.longValue());
        return keyMap;
    }

    @Override
    public void addResult(Object result) {
        if (result != null) {
            this.results.add(result);
        }
        this.latch.countDown();
    }
}
