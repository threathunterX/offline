package com.threathunter.bordercollie.slot.api;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.variable.DimensionType;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by toyld on 2/13/17.
 */
public class ContinuousDataHelper implements DataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ContinuousDataHelper.class);
    private static final String DB_NAME = "offline";
    private static final int BUFFER_LIMIT = 1000;
    private static ClientPolicy policy = new ClientPolicy();
    private static CommonDynamicConfig config = CommonDynamicConfig.getInstance();
    private static final int port = config.getInt("aerospike_port", 3000);
    private static final String host = config.getString("aerospike_address", "127.0.0.1");
    public static final AerospikeClient AEROSPIKE_CLIENT = new AerospikeClient(policy, host, port);
    private static Map<DimensionType, List> validVars = new HashMap<>();

    static {
        List<String> ipVarList = new ArrayList<>();
        ipVarList.add("ip__visit_dynamic_distinct_count_did__1h__slot"); // ip 关联did数
        ipVarList.add("ip__visit_dynamic_distinct_count_uid__1h__slot");// ip 关联user数
        ipVarList.add("ip__visit_dynamic_distinct_count_page__1h__slot"); // ip 关联page数
        ipVarList.add("ip__visit_incident_count__1h__slot"); //ip 风险事件数
        ipVarList.add("ip__visit_dynamic_count__1h__slot");
        validVars.put(DimensionType.IP, ipVarList);

        List<String> didVarList = new ArrayList<>();
        didVarList.add("did__visit_dynamic_distinct_count_ip__1h__slot"); //did 关联ip数
        didVarList.add("did__visit_dynamic_distinct_count_uid__1h__slot"); //did 关联user数
        didVarList.add("did__visit_dynamic_distinct_count_page__1h__slot"); //did 关联page数
        didVarList.add("did__visit_incident_count__1h__slot"); //did 风险事件数
        didVarList.add("did__visit_dynamic_count__1h__slot");
        validVars.put(DimensionType.DID, didVarList);

        List<String> userVarList = new ArrayList<>();
        userVarList.add("uid__visit_dynamic_distinct_count_ip__1h__slot");//user 关联ip数
        userVarList.add("uid__visit_dynamic_distinct_count_did__1h__slot"); //user 关联did数
        userVarList.add("uid__visit_dynamic_distinct_count_page__1h__slot"); //user 关联page数
        userVarList.add("uid__visit_incident_count__1h__slot"); //user 风险事件数
        userVarList.add("uid__visit_dynamic_count__1h__slot");


        validVars.put(DimensionType.UID, userVarList);
        List<String> pageVarList = new ArrayList<>();
        pageVarList.add("page__visit_dynamic_distinct_count_ip__1h__slot"); // page 关联ip数
        pageVarList.add("page__visit_dynamic_distinct_count_uid__1h__slot"); //page 关联user数
        pageVarList.add("page__visit_dynamic_distinct_count_did__1h__slot"); //page 关联did数
        pageVarList.add("page__visit_incident_count__1h__slot"); // page 风险事件数
        pageVarList.add("page__visit_dynamic_count__1h__slot");
        validVars.put(DimensionType.PAGE, pageVarList);
        List<String> globalVarList = new ArrayList<>();
        globalVarList.add("global__visit_dynamic_distinct_count_ip__1h__slot"); //ip数
        globalVarList.add("global__visit_dynamic_distinct_count_did__1h__slot"); //did 数
        globalVarList.add("global__visit_dynamic_distinct_count_uid__1h__slot"); // user 数
        globalVarList.add("global__visit_incident_distinct_count_uid__1h__slot"); // 风险用户数
        globalVarList.add("global__visit_incident_count__1h__slot"); // 风险事件数
        globalVarList.add("global__visit_dynamic_count__1h__slot"); // 策略管理页面右上角 总点击数
        globalVarList.add("global__marketing_incident_count__1h__slot"); // 策略管理，其他风险数
        globalVarList.add("global__marketing_count__1h__slot"); // 策略管理，其他风险数
        globalVarList.add("global__order_submit_count__1h__slot"); // 策略管理，其他风险数
        globalVarList.add("global__order_submit_incident_count__1h__slot"); // 策略管理，其他风险数
        globalVarList.add("global__transaction_withdraw_count__1h__slot"); // 策略管理，其他风险数
        globalVarList.add("global__transaction_withdraw_incident_count__1h__slot"); // 策略管理，其他风险数
        validVars.put(DimensionType.GLOBAL, globalVarList);
    }

    private final HashMap<Key, Bin> addBuffer = new HashMap<>();
    private final String workingHourStr;
    private final String workingHourTS;
    private String timestampOfDay;

    public ContinuousDataHelper() {
        // if workingHourStr is Empty, it can't store to Aerospike, but it can be use to query.
        policy.readPolicyDefault.timeout = 100;
        policy.readPolicyDefault.maxRetries = 3;
        policy.readPolicyDefault.sleepBetweenRetries = 10;
        this.workingHourStr = "";
        this.workingHourTS = "";
        this.timestampOfDay = "";
        // todo flag to disable add method, if it's query server mode
    }

    public ContinuousDataHelper(String WorkingHourStr) {
        policy.writePolicyDefault.expiration = 3600 * 24 * 60; // 60 day expire
        policy.writePolicyDefault.sendKey = true;
        policy.writePolicyDefault.timeout = 300;
        policy.writePolicyDefault.maxRetries = 3;
        policy.writePolicyDefault.sleepBetweenRetries = 50;

        this.workingHourStr = WorkingHourStr;
        this.workingHourTS = SlotUtils.getWorkingHourTSFromStr(this.workingHourStr);
        this.timestampOfDay = SlotUtils.getTimestampForStartOfDay(workingHourTS.substring(0, 10));
        logger.debug("timestamp of the day: " + this.timestampOfDay);
    }

    public void add(Key key, Bin bin) {
        try {
            AEROSPIKE_CLIENT.put(policy.writePolicyDefault, key, bin);
            logger.info(">>>>>>aerospike entry--> \t key:{}, value:{}", key.toString(), bin.toString());
        } catch (AerospikeException e) {
            logger.error("Continuous data write to aerospike error, key: " + key, e);
            SlotMetricsHelper.getInstance().addMetrics("store.continuous.timeout", 1.0);
            return;
        }
        SlotMetricsHelper.getInstance().addMetrics("store.continuous.success", 1.0);
    }

    public void add(String key, String dimension, String timestamp, Map<String, Integer> vars) {
        // timestamp
        key = String.format("%s_%s", key, this.timestampOfDay);
        Key db_key = new Key(DB_NAME, dimension, key);
        Bin bin = new Bin(timestamp, vars);
        add(db_key, bin);
    }

    public void add_batch(Key db_key, Bin bin) {
        logger.info("aerospike key:{},bin:{}", new String(db_key.toString()), bin.toString());
        addBuffer.put(db_key, bin);
        if (addBuffer.size() >= BUFFER_LIMIT) {
            flush();
        }
    }

    public void add_batch(String key, String dimension, String timestamp, Map<String, Integer> vars) {
        Key db_key = new Key(DB_NAME, dimension, String.format("%s_%s", key, SlotUtils.getTimestampForStartOfDay(timestamp)));
        Bin bin = new Bin(timestamp, vars);
        // below is nessnary? or just add?
        add_batch(db_key, bin);
    }

    public void start() {

    }

    public void stop() {
        logger.info("ZJP.ContinuousDataHelper.stop");
        this.flush();
    }

    public void flush() {
        addBuffer.entrySet().forEach(e -> add(e.getKey(), e.getValue()));
        SlotMetricsHelper.getInstance().addMetrics("continusous.flush", (double) addBuffer.size());
        addBuffer.clear();
    }

    public Map<String, Map<String, Integer>> query_many(String key, String dimension, String[] timestamps, String[] var_list) {
        // query Continuous hour's var_list's data.
        HashMap result = new HashMap();
        dimension = formalize(dimension);
        Key db_key = new Key(DB_NAME, dimension, key);
        List<BatchRead> batchRead = new ArrayList<>();
        batchRead.add(new BatchRead(db_key, timestamps));

        // find days between timestamps.
        Set<String> timestampOfDays = new HashSet<>();
        for (int i = 0; i < timestamps.length; i++) {
            timestampOfDays.add(SlotUtils.getTimestampForStartOfDay(timestamps[i].substring(0, 10)));
        }

        for (String tsd : timestampOfDays) {
            Key tmp_db_key = new Key(DB_NAME, dimension, String.format("%s_%s", key, tsd));
            batchRead.add(new BatchRead(tmp_db_key, timestamps));
        }

        AEROSPIKE_CLIENT.get(null, batchRead);
        for (BatchRead record : batchRead) {
            Key k = record.key;
            Record rec = record.record;
            if (rec != null && rec.bins != null) {
                for (String binName : timestamps) {
                    HashMap<String, Map> tMap = new HashMap<>();
                    HashMap<String, Integer> bMap = (HashMap) rec.bins.get(binName);
                    if (bMap == null) {
                        logger.warn(String.format("key %s, bins don't have %s", db_key, binName));
                        continue;
                    }
                    logger.warn(String.format("key %s, bin's Map: %s", db_key, bMap.toString()));
                    // here is just filter keys from hashmap todo
                    for (String var_name : var_list) {
                        Map<String, Object> map = new HashMap<>();
                        try {
                            map.put("key", Double.valueOf(binName).longValue() * 1000);
                        } catch (Exception e) {
                            logger.error("fail to convert to timestamp " + binName, e);
                            map.put("key", binName);
                        }
                        map.put("value", bMap.getOrDefault(var_name, 0));
                        tMap.put(var_name, map);
                    }
                    result.put(binName, tMap);
                }
            }
//            else {
//                logger.info(String.format("Record not found: ns=%s set=%s key=%s",
//                        k.namespace, k.setName, k.userKey));
//            }

        }
        return result;
    }

    private String formalize(String dimension) {
        try {
            DimensionType type = DimensionType.getDimension(dimension);
            if (type != null)
                return type.name();
        } catch (Exception e) {
            logger.error("dimension is not exist, dimension = {}", dimension);
        }
        return dimension;
    }

    public Map<String, Object> query(String key, String dimension, String timestamp, String[] var_list) {
        HashMap result = new HashMap();
        Key db_key = new Key(DB_NAME, dimension, key);
        Record record = AEROSPIKE_CLIENT.get(null, db_key, timestamp);

        if (record == null || record.bins == null) {
            return result;
        }
        HashMap<String, Object> bMap = (HashMap) record.bins.get(timestamp);
        for (String var_name : var_list) {
            result.put(var_name, bMap.get(var_name));
        }
        return result;
    }

    /**
     * Format a key's statistic cache hashmap to Continuous storage convenient.
     *
     * @param o a HashMap which should have key: key, dimension, and bunch of variables
     * @return com.threathunter.nebula.slot.offline.ContinuousDataObj
     **/
    public ContinuousDataObj format(Map o) {
        if (workingHourStr.isEmpty() || o == null) {
            // todo throws exception should init with working ms
            return null;
        }
        DimensionType dimensionType = DimensionType.getDimension((String) o.get("dimension"));
        logger.trace("aerospike key:dbname:{},dimensionTypeName:{},key:{}", DB_NAME, dimensionType.name(), String.format("%s_%s", o.get("key"), this.timestampOfDay));
        Key key = new Key(DB_NAME, dimensionType.name(), String.format("%s_%s", o.get("key"), this.timestampOfDay));
        Map storeMap = new HashMap<>();
        if (validVars.get(dimensionType) == null) {
            logger.error("aerospike error for dimension : {} , key:{}", dimensionType.name(), String.format("%s_%s", o.get("key"), this.timestampOfDay));
            return null;
        } else {
            validVars.get(dimensionType).forEach(var_name -> {
                if (var_name == null) {
                    logger.info("varname is null");
                    return;
                }
                Object stat = o.get(var_name);
                if (stat == null)
                    storeMap.put(var_name, 0);
                if (stat instanceof Number) {
                    storeMap.put(var_name, stat);
                }
                if (stat instanceof List) {
                    storeMap.put(var_name, ((List) stat).size());
                }
                if (stat instanceof Set) {
                    storeMap.put(var_name, ((Set) stat).size());
                }
            });
        }

        logger.trace("aerospike value: ts:{},map:{} ", this.workingHourTS, storeMap);
        Bin bin = new Bin(this.workingHourTS, storeMap);
        ContinuousDataObj dataObj = new ContinuousDataObj(key, bin);
        return dataObj;
    }

    public void store(final Map mapData) {
        logger.debug("ZJP.ContinuousDataHelper.store: " + new Gson().toJson(mapData));
        ContinuousDataObj dataObj = this.format(mapData);
        if (dataObj == null) {
            return;
        }
        add_batch(dataObj.getKey(), dataObj.getValue());
    }
}
