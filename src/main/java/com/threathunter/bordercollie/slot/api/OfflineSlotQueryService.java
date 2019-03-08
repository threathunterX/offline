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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by toyld on 5/6/17.
 */
public class OfflineSlotQueryService implements Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineSlotQueryService.class);
    private final ServiceMeta meta;
    private OfflineSlotDataHelper offlineSlotDataHelper;

    public OfflineSlotQueryService(boolean redisMode) {
        if (redisMode) {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("OfflineSlotVariableQuery_redis.service");
        } else {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_keystatquery_rmq.service");
        }
    }

    @Override
    public Event process(Event event) {
        LOGGER.info(">>>input event:{}", event);
        // The Query conditions just for Exception output.
        String dimension = null;
        Double timestamp = null;
        List<String> var_list = null;
        List<String> keys = null;
        String queryHour = null;
        try {
            MetricsHelper.getInstance().addMetrics("nebula.offline", "rpc.keystatquery", 1.0);
            Map<String, Object> properties = event.getPropertyValues();
            keys = (List) properties.get("keys");


            dimension = (String) properties.get("dimension");

            var_list = (List<String>) properties.get("var_list");
            Object obj = properties.get("timestamp");
            if (obj instanceof Long)
                timestamp = new Double((Long) obj);
            else if (obj instanceof Double)
                timestamp = (Double) properties.get("timestamp");

            Timestamp t = new Timestamp(timestamp.longValue());
            queryHour = new DateTime(t.getTime()).toString(SlotUtils.slotTimestampFormat).toString();

            String logPath = CommonDynamicConfig.getInstance().getString("persist_path") + "/" + queryHour + "/" + CommonDynamicConfig.getInstance().getString("offline.db.name", "data");
            if (!(new File(logPath).exists())) {
                LOGGER.warn("log path is not exist: " + logPath);
                return new Event();
            }

            this.offlineSlotDataHelper = new OfflineSlotDataHelper(t.getTime(), true);
            Map<String, Object> outer = new HashMap<>();
            for (String key : keys) {
                Map<String, Object> inner = offlineSlotDataHelper.getStatistic(key, dimension, var_list);
                String varStr = concat(var_list);
                if (inner == null || inner.isEmpty()) {
                    LOGGER.info("result from level db is null, key = {} , dimension = {} ,var_list = {}", key, dimension, varStr);
                    outer.put(key, new HashMap<String, Object>());
                } else
                    outer.put(key, inner);

            }
            HashMap responseProperties = new HashMap();
            responseProperties.put("result", outer);
            Event response = new Event("nebula", "offline_keystatquery_response",
                    event.getKey(), SystemClock.getCurrentTimestamp(), 1.0, responseProperties);
            LOGGER.info(">>>output event:{}", response.toString());
            return response;
        } catch (Exception e) {
            LOGGER.error(String.format("Offline Slot Query Error, Query Statements: keys:%s\ndimension:%s\ntimestamp:%s\ntimestampHour:%s\nvar_list:%s",
                    concat(keys), dimension, timestamp, queryHour, var_list), e);
            return null;
        }
    }

    private String concat(List<String> var_list) {
        StringBuilder builder = new StringBuilder();
        for (String string : var_list)
            builder.append(string);
        return builder.toString();
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
