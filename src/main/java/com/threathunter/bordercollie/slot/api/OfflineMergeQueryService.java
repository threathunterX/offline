package com.threathunter.bordercollie.slot.api;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.Service;
import com.threathunter.bordercollie.slot.util.MetricsHelper;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.bordercollie.slot.util.SystemClock;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.EventMeta;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yy on 17-12-8.
 */
public class OfflineMergeQueryService implements Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineMergeQueryService.class);
    private final ServiceMeta meta;

    public OfflineMergeQueryService(boolean redisMode) {
        if (redisMode) {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_merged_variable_query_redis.service");
        } else {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_merged_variable_query_rmq.service");
        }
    }

    @Override
    public Event process(Event event) {
        LOGGER.info(">>>Merge input event:{}", event);
        String dimension = null;
        List<String> var_list = null;
        List<String> keys = null;
        Map<Long, String> queryHours = new HashMap<>();
        MetricsHelper.getInstance().addMetrics("nebula.offline", "rpc.merge.query", 1.0);
        try {
            Map<String, Object> properties = event.getPropertyValues();
            keys = (List) properties.get("keys");
            dimension = (String) properties.get("dimension");
            var_list = (List<String>) properties.get("var_list");
            List<Long> times = (List<Long>) properties.get("time_list");
            times.forEach(time -> queryHours.put(time, new DateTime(time).toString(SlotUtils.slotTimestampFormat)));
        } catch (Exception e) {
            LOGGER.error("merge query input error, event = " + event.toString(), e);
            Event eventNew = new Event();
            LOGGER.info(">>>out put event : {}", eventNew);
            return eventNew;
        }
        try {
            OfflineMergeDataQuery query = new OfflineMergeDataQuery(dimension, keys, queryHours, var_list);
            Map<String, Map<String, Object>> data = query.getData();
            HashMap responseProperties = new HashMap();
            responseProperties.put("result", data);
            Event response = new Event("nebula", "offline_merge_variablequery",
                    event.getKey(), SystemClock.getCurrentTimestamp(), 1.0, responseProperties);
            LOGGER.info(">>>output event:{}", response.toString());
            return response;

        } catch (Exception e) {
            LOGGER.error("fail to process event :\t" + event.toString(), e);
            LOGGER.info(">>>output event:NULL");
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
