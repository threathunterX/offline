package com.threathunter.bordercollie.slot.api;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.Service;
import com.threathunter.bordercollie.slot.util.MetricsHelper;
import com.threathunter.bordercollie.slot.util.SystemClock;
import com.threathunter.model.Event;
import com.threathunter.model.EventMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class ContinuousQueryService implements Service {
    private final ServiceMeta meta;
    Logger logger = LoggerFactory.getLogger(ContinuousQueryService.class);
    private ContinuousDataHelper continuousDataHelper;

    public ContinuousQueryService(boolean redisMode) {
        if (redisMode) {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_continuousquery_redis.service");
        } else {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_continuousquery_rmq.service");
        }
        this.continuousDataHelper = new ContinuousDataHelper();
    }

    @Override
    public Event process(Event event) {
        try {
            logger.info(">>>input event:{}", event);
            MetricsHelper.getInstance().addMetrics("nebula.offline", "rpc.continuousquery", 1.0);
            Map<String, Object> properties = event.getPropertyValues();

            String key = (String) properties.get("key");
            String dimension = (String) properties.get("dimension");
            List<String> timestamp_list = (List<String>) properties.get("timestamps");
            String[] timestamps = new String[timestamp_list.size()];
            timestamp_list.toArray(timestamps);
            List<String> variable_list = (List<String>) properties.get("var_list");
            String[] variables = new String[variable_list.size()];
            variable_list.toArray(variables);
            Map<String, Map<String, Integer>> result = this.continuousDataHelper.query_many(key, dimension, timestamps, variables);
            if (result == null || result.isEmpty()) {
                Event eventNew = new Event();
                logger.info("<<<output event:{}", eventNew);
                return eventNew;
            }
            HashMap responseProperties = new HashMap();
            if (result == null || result.isEmpty()) {
                Event e = new Event();
                logger.info("<<<output event:{}", e);
                return e;
            }
            responseProperties.put("result", result);
            Event response = new Event("nebula", "continuousquery_response",
                    event.getKey(), SystemClock.getCurrentTimestamp(), 1.0, responseProperties);
            logger.info("<<<output event:{}", response);
            return response;
        } catch (Exception e) {
            logger.error(String.format("fail to query, input event:%s", event.toString()), e);
            logger.info("<<<output event:NULL");
            return null;
        }
    }

    @Override
    public ServiceMeta getServiceMeta() {
        return this.meta;
    }

    @Override
    public EventMeta getRequestEventMeta() {
        return null;
    }

    @Override
    public EventMeta getResponseEventMeta() {
        return null;
    }

    @Override
    public void close() {

    }
}
