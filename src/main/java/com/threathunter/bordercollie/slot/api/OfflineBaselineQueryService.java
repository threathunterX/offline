package com.threathunter.bordercollie.slot.api;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.Service;
import com.threathunter.bordercollie.slot.util.ConstantsUtil;
import com.threathunter.bordercollie.slot.util.MetricsHelper;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.EventMeta;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Timestamp;
import java.util.*;

/**
 * 
 */
public class OfflineBaselineQueryService implements Service {
    private final ServiceMeta meta;
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineBaselineQueryService.class);

    public OfflineBaselineQueryService(boolean redisMode) {
        if (redisMode) {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_baselinekeystatquery_redis.service");
        } else {
            this.meta = ServiceMetaUtil.getMetaFromResourceFile("offline_baselinekeystatquery_rmq.service");
        }
    }

    @Override
    public Event process(Event event) {
        int count = 100;
        int topCount = 100;
        OfflineSlotDataHelper offlineSlotDataHelper = null;
        String app = null;
        List<String> variableNames = null;
        String timestamp = null;
        List<String> mergeList = null;
        List<String> keyVariables = null;
        String keyDimension = null;
        String queryHour = null;
        try {
            MetricsHelper.getInstance().addMetrics("nebula.offline", "rpc.baselinekeystatquery", 1.0);
            Map<String, Object> properties = event.getPropertyValues();

            app = (String) properties.get("app");
            variableNames = (List<String>) properties.get("var_list");
            timestamp = properties.get("timestamp").toString();
            Timestamp t = new Timestamp(Long.parseLong(timestamp));
            queryHour = new DateTime(t.getTime()).toString(SlotUtils.slotTimestampFormat).toString();

            mergeList = properties.get("merge_list") == null ? new ArrayList<>() : (List<String>) properties.get("merge_list");
            Set<String> mergeVariables = new HashSet<>(mergeList);

            keyVariables = (List<String>) properties.get("key_variable");
//        List<String> keyVariables = new ArrayList<>();
//        keyVariables.add(keyVariable);
            keyDimension = properties.get("key_dimension").toString();
            String key = event.getKey();
            if (key.isEmpty()) {
                key = SlotUtils.totalKey;
            }
            if (properties.containsKey("count")) {
                if (properties.get("count") instanceof String) {
                    count = Integer.parseInt(properties.get("count").toString());
                } else {
                    count = ((Number) properties.get("count")).intValue();
                }
            }
            if (properties.containsKey("topCount")) {
                topCount = Integer.parseInt(properties.get("topCount").toString());
            }

            String log_path = CommonDynamicConfig.getInstance().getString("persist_path") + "/" + queryHour + "/" + CommonDynamicConfig.getInstance().getString("offline.db.name", "data");
            if (!(new File(log_path).exists())) {
                LOGGER.warn("log path is not exist: " + log_path);
                return new Event();
            }
            offlineSlotDataHelper = new OfflineSlotDataHelper(Long.parseLong(timestamp) / ConstantsUtil.HOUR_MILLIS * ConstantsUtil.HOUR_MILLIS, true);


            Map<String, Map<String, Object>> resultData = new HashMap<>();
            Map<String, Object> mergeData = new HashMap<>();
            Map<String, Object> baseData = offlineSlotDataHelper.getStatistic(key, keyDimension, keyVariables);
            if (baseData == null) {
                return null;
            }

            // do count cut, make sure there is not too much key to query
            LinkedList<Map.Entry<String, Object>> mapEntry = new LinkedList<>();
            mapEntry.addAll(((Map<String, Object>) baseData.getOrDefault(keyVariables.get(0), new HashMap<>())).entrySet());

            Collections.sort(mapEntry, (a, b) -> {
                double ret = ((Number) a.getValue()).doubleValue() - ((Number) b.getValue()).doubleValue();
                return ret > 0 ? -1 : ret < 0 ? 1 : 0;
            });

            Integer count_flag = 0;
            for (Map.Entry<String, Object> entry : mapEntry) {
                Map<String, Object> subVariableDataMap = new HashMap<>();
                subVariableDataMap.put(keyVariables.get(0), entry.getValue());
                resultData.put(entry.getKey(), subVariableDataMap);
                count_flag++;
                if (count_flag >= count) {
                    break;
                }
            }

            List<String> keys = new ArrayList<>(resultData.keySet());
            variableNames.remove(keyVariables.get(0));
            // get every variable data for the base key, and do merge if need
            final OfflineSlotDataHelper finalOfflineSlotDataHelper = offlineSlotDataHelper;
            final List<String> finalVariableNames = variableNames;
            final String finalKeyDimension = keyDimension;
            keys.forEach(k -> {
                try {
                    Map<String, Object> allSubResult = finalOfflineSlotDataHelper.getStatistic(k, finalKeyDimension, finalVariableNames);
                    finalVariableNames.forEach(variable -> {
                        if (mergeVariables.contains(variable)) {
                            //do merge
                            Map<String, Object> Merged = (Map<String, Object>) mergeData.getOrDefault(variable, new HashMap<>());
                            Map<String, Object> toMerge = (Map<String, Object>) allSubResult.getOrDefault(variable, new HashMap<>());
                            Merged = SlotUtils.mergeMap(toMerge, Merged);
                            mergeData.put(variable, Merged);
                        } else {
                            Map<String, Object> variableMap = resultData.get(k);
                            variableMap.put(variable, allSubResult.get(variable));
                        }
                    });
                } catch (Exception e) {
//                System.out.println();
                    e.printStackTrace();
                }
            });

            // topcount cut the merge variables.
            for (String var : mergeData.keySet()) {
                LinkedList<Map.Entry<String, Number>> mergemapEntry = new LinkedList<>();
                mergemapEntry.addAll(((Map<String, Number>) mergeData.get(var)).entrySet());
                Collections.sort(mergemapEntry, new Comparator<Map.Entry<String, Number>>() {
                    public int compare(Map.Entry<String, Number> a, Map.Entry<String, Number> b) {
                        try {
                            if (a.getValue().doubleValue() > b.getValue().doubleValue())
                                return -1;
                            else
                                return 1;
                        } catch (Exception e) {
                            return 1;
                        }
                    }
                });
//                mergeData.remove(var);
                Map<String, Number> varMap = new HashMap<>();
                count_flag = 0;
                for (Map.Entry<String, Number> entry : mergemapEntry) {
                    varMap.put(entry.getKey(), entry.getValue());
                    count_flag++;
                    if (count_flag >= topCount) {
                        break;
                    }
                }
                mergeData.put(var, varMap);
            }
            Map<String, Object> responseProperties = new HashMap<>();
            responseProperties.put("result", resultData);
            responseProperties.put("merges", mergeData);

            Event response = new Event("nebula", "baselinekeystatquery_response",
                    event.getKey(), System.currentTimeMillis(), 1.0, responseProperties);
            return response;
        } catch (Exception e) {
            LOGGER.warn(String.format("Offline Slot Query Error, Query Statements: key_variables:%s\nkey_dimension:%s\ntimestamp:%s\ntimestampHour:%s\nvar_list:%s\nmerge_list:%s",
                    keyVariables, keyDimension, timestamp, queryHour, variableNames, mergeList));
            e.printStackTrace();
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
