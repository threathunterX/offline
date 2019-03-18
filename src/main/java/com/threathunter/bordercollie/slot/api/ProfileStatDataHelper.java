package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.ResultFormatter;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class ProfileStatDataHelper implements DataConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProfileStatDataHelper.class);
    private static Integer keyLimit = 100; // send per 100 key.
    private final String workingHourStr;
    private final long timestamp;
    private ProfileStatBabelSender profileStatBabelSender = ProfileStatBabelSender.getInstance();
    private Integer keyCount = 0;
    private Integer flushCount = 0;
    private HashMap<String, Map<String, Object>> propertyCache = new HashMap<>(); // dimension: variable : [ profiledataObj, ..]
    // Variable Name Map, Slot to Profile.
    private Map<String, List<String>> slotProfileMetaMap; // dimension: slot_var: [profile_var, type]

    public ProfileStatDataHelper(final String workingHourStr) throws FileNotFoundException {
        this.workingHourStr = workingHourStr;
        propertyCache.put("did", new HashMap<>());
        propertyCache.put("uid", new HashMap<>());
        propertyCache.put("ip", new HashMap<>());
        propertyCache.put("page", new HashMap<>());
        propertyCache.put("global", new HashMap<>());
        boolean enableCrawler = CommonDynamicConfig.getInstance().getBoolean("crawler_enable", true);
        InputStream inputStream;
        if (enableCrawler) {
            inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("SlotToProfileVarMap.json");
            LOGGER.warn("enable crawler data send");
        } else {
            inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("SlotToProfileVarMap_NoCrawler.json");
            LOGGER.warn("disable crawler data send");
        }
        Gson gson = new Gson();
        try {
            this.timestamp = new SimpleDateFormat("yyyyMMddHH").parse(this.workingHourStr).getTime();
        } catch (Exception e) {
            throw new RuntimeException("parse working hour string error", e);
        }
        try {
            slotProfileMetaMap = gson.fromJson(new InputStreamReader(inputStream), Map.class);
        } catch (Exception e) {
            throw new RuntimeException("slot profile meta parse", e);
        }
    }

    public static Map<String, Object> parse(long ts, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put("key", ts);
        map.put("value", value);
        if (value instanceof List) {
            List<Map> list = (List) value;
            Map<String, Object> innerMap = new HashMap<>();
            for (Map iMap : list) {
                String k = (String) iMap.get("key");
                Object v = iMap.get("value");
                innerMap.put(k, v);
            }
            map.put("value", innerMap);
        }
        return map;
    }

    public void start() {
        String babelMode = CommonDynamicConfig.getInstance().getString("babel_server", "redis");
        Boolean isRedis;
        if (babelMode.equals("redis")) {
            isRedis = true;
        } else {
            isRedis = false;
        }
        this.profileStatBabelSender.start(isRedis);
    }

    public void stop() {
        LOGGER.debug("ZJP.ProfileStatDataHelper.stop");
        if (this.keyCount > 0) {
            flush();
        }
        LOGGER.info("success flush keyCount: " + this.flushCount);
        this.profileStatBabelSender.stop();
    }

    public void store(final Map mapData) {
        LOGGER.debug("ProfileStatDataHelper store: " + new Gson().toJson(mapData));
        ProfileStatDataObj dataObj = this.format(mapData);
        /**
         * Event should look like this. Event("nebula", "VISIT", "", now, properties)
         *  $properties: { dimension: { key : {var_name1: count, var_name2: count}}} every 100 key send once.
         *
         * new;
         *  {dimension : {var1: [ ProfileStatDataObj ] }}
         *   send if any var like var1's list size reach 100
         * **/
        if (dataObj == null) {
            return;
        }
        String dataDimension = dataObj.getDimension();
        if (!propertyCache.containsKey(dataDimension)) {
            LOGGER.error("unknown dimension: " + dataDimension);
            return;
        }
        Map<String, Object> limitedSendMap = propertyCache.get(dataDimension);
        limitedSendMap.put(dataObj.getKey(), dataObj.getValue());
        if (limitedSendMap.size() >= this.keyLimit) {
            Map<String, Object> sendData = limitedSendMap;
            flush(dataDimension, sendData);
            propertyCache.put(dataDimension, new HashMap<>());
        }
    }

    public void flush(final String dimension, final Map<String, Object> sendData) {
        Map<String, Object> eventProperty = new HashMap<>();
        eventProperty.put(dimension, sendData);
        Event e = new Event("nebula", "VISIT", "", System.currentTimeMillis(), 1.0, eventProperty);
        LOGGER.info("ProfileStatDataHelper flush:", new Gson().toJson(e));
        profileStatBabelSender.send(e);
        SlotMetricsHelper.getInstance().addMetrics("profilestat.flush", 1.0);
    }

    public void flush() {
        // flush the rest of data at last.
        this.propertyCache.forEach((dimension, variableMap) -> {
            if (variableMap.size() > 0) {
                flush(dimension, variableMap);
            }
        });
    }

    public Object getDataFromStr(final String type) {
        if (type.equals("int")) {
            return 0;
        } else if (type.equals("list")) {
            return new ArrayList<>();
        } else if (type.equals("str")) {
            return "";
        } else if (type.equals("map")) {
            return new HashMap<>();
        }
        return null;
    }

    public ProfileStatDataObj format(final Map allDataPerKey) {

        // o should have key: dimension, key, and var_names
        if (!allDataPerKey.containsKey("key") || !allDataPerKey.containsKey("dimension")) {
            LOGGER.error("ProfileStatDataObj format error(not found key):" + new Gson().toJson(allDataPerKey));
            return null;
        }

        String dimension = (String) allDataPerKey.get("dimension");
        String key = (String) allDataPerKey.get("key");
        try {
            if (key.equals(SlotUtils.totalKey) && !dimension.equals("global")) {
                return null;
            }
            if (!slotProfileMetaMap.containsKey(dimension)) {
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("prepare profile data from slot error:" + new Gson().toJson(allDataPerKey), e);
        }

        List<String> variableMetaMap = slotProfileMetaMap.get(dimension);
        HashMap<String, Object> toProfileVariableData = new HashMap<>();
        try {
            variableMetaMap.stream().forEach(variable -> {
                Object oneVariableDataOfKey = allDataPerKey.get(variable);
                Map<String, Object> map = null;
                if ("page".equals(dimension))
                    map = ResultFormatter.parse(this.timestamp, oneVariableDataOfKey);
                else
                    map = parse(this.timestamp, oneVariableDataOfKey);
                if (oneVariableDataOfKey != null) {
                    toProfileVariableData.put(variable, map);
                }
            });
        } catch (Exception e) {
            LOGGER.error("prepare profile data from slot error:" + new Gson().toJson(allDataPerKey), e);
        }
        ProfileStatDataObj dataObj = new ProfileStatDataObj();
        dataObj.setDimension(dimension);
        dataObj.setValue(toProfileVariableData);
        dataObj.setKey(key);
        return dataObj;
    }
}
