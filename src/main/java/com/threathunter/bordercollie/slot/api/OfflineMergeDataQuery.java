package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.google.gson.Gson;
import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yy on 17-12-8.
 */
public class OfflineMergeDataQuery {
    private static Logger logger = LoggerFactory.getLogger(OfflineMergeDataQuery.class);
    private static String GLOBAL_KEY = "__GLOBAL__";
    private final List<String> varList;
    private final String dimension;
    private final List<String> keys;
    private final Map<Long, String> queryHours;
    private QueryType queryType;
    private List<DB> dbs;
    private SlotUtils slotUtils = new SlotUtils();

    public OfflineMergeDataQuery(String dimension, List<String> keys, Map<Long, String> queryHours, List<String> varList) {
        this.dimension = dimension;
        this.keys = keys;
        this.queryHours = queryHours;
        this.varList = varList;
        queryType = QueryType.OTHER;
        initdbs();
    }

    public OfflineMergeDataQuery(String dimension, Map<Long, String> queryHours, List<String> varList) {
        this.dimension = dimension;
        this.keys = null;
        this.queryHours = queryHours;
        this.varList = varList;
        queryType = QueryType.GLOBAL;
        initdbs();
    }

    private void initdbs() {
        dbs = new ArrayList<>();
        for (Map.Entry<Long, String> entry : queryHours.entrySet()) {
            try {
                Long ts = entry.getKey();
                String queryHour = entry.getValue();
                String logPath = CommonDynamicConfig.getInstance().getString("persist_path") + "/" + queryHour + "/" + CommonDynamicConfig.getInstance().getString("offline.db.name", "data");
                if (!(new File(logPath).exists())) {
                    logger.warn("log path is not exist: " + logPath);
                    continue;
                }
                DB db = OfflineLevelDbCache.getInstance().getLevelDb(ts, false);
                dbs.add(db);

            } catch (Exception e) {
                logger.error("create db error, timestamp = {},\tlogPath = {}", entry.getKey(), entry.getValue());
            }
        }
    }

    public Map<String, Map<String, Object>> getData() {
        Map<String, List<Map<String, Object>>> tmp = new HashMap<>();
        if ("global".equals(dimension)) {
            keys.clear();
            keys.add(GLOBAL_KEY);
        }
        for (String key : keys) {
            byte[] dbKey = slotUtils.get_stat_key(key, dimension);
            List<Map<String, Object>> crossTimeList = new ArrayList<>();
            for (DB db : dbs) {
                byte[] dbValue = db.get(dbKey);
                if (dbKey == null || dbValue == null) {
                    continue;
                }
                Gson gson = new Gson();
                Map<String, Object> inputAllDataPerkey = gson.fromJson(new String(dbValue), Map.class);
                Map<String, Object> output = new HashMap<>();
                for (Map.Entry<String, Object> entry : inputAllDataPerkey.entrySet()) {
                    String variable = entry.getKey();
                    Object value = entry.getValue();
                    if (variable == null || varList == null || !varList.contains(variable))
                        continue;
                    output.put(variable, value);
                }
                crossTimeList.add(output);
            }
            tmp.put(key, crossTimeList);
        }
        Map<String, Map<String, Object>> ret = new HashMap<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : tmp.entrySet()) {
            String key = entry.getKey();
            List<Map<String, Object>> value = entry.getValue();
            Map<String, Object> mergeValue = toMerge(value);
            ret.put(key, mergeValue);
        }
        return ret;
    }

    private Map<String, Object> toMerge(List<Map<String, Object>> mergeList) {
        Map<String, Object> ret = new HashMap<>();
        for (String var : varList) {
            Object valueForMerge = null;
            for (Map<String, Object> map : mergeList) {
                Object o = map.get(var);
                if (o instanceof Double) {
                    if (valueForMerge == null)
                        valueForMerge = new Double(0.0);
                    Double tmp = (Double) valueForMerge;
                    valueForMerge = tmp.doubleValue() + (Double) o;
                }
                if (o instanceof List) {
                    if (valueForMerge == null)
                        valueForMerge = new ArrayList<>();
                    List tmp = (List) valueForMerge;
                    tmp.addAll((List) o);
                    valueForMerge = tmp;
                }
                if (o instanceof Integer) {
                    if (valueForMerge == null)
                        valueForMerge = new Integer(0);
                    Integer tmp = (Integer) valueForMerge;
                    valueForMerge = tmp.intValue() + (Integer) 0;
                }
                if (o instanceof Long) {
                    if (valueForMerge == null)
                        valueForMerge = new Long(0);
                    Long tmp = (Long) valueForMerge;
                    valueForMerge = tmp.longValue() + (Long) o;
                }
                if (o instanceof Float) {
                    if (valueForMerge == null)
                        valueForMerge = new Float(0);
                    Float tmp = (Float) valueForMerge;
                    valueForMerge = tmp.floatValue() + (Float) o;
                }
            }
            ret.put(var, valueForMerge);
        }
        return ret;
    }


    enum QueryType {
        GLOBAL, OTHER
    }


}
