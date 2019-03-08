package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by toyld on 3/1/17.
 */
public class IncidentDataHelper implements DataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(IncidentDataHelper.class);
    private IncidentBabelSender incidentBabelSender = IncidentBabelSender.getInstance();
    private final List<String> incidentVarList = new ArrayList<>();
    private static String incidentApp = "nebula";
    private Integer flush_count = 0;

    public IncidentDataHelper() {
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip__visit_incident_max_rate__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip__visit_incident_score__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip__visit_incident_first_timestamp__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip_uid__visit_dynamic_count_top20__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip_page__visit_dynamic_count_top20__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip_did__visit_dynamic_count_top20__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip_tag__visit_incident_count_top20__1h__slot"));
//        incidentVarList.add(Identifier.fromKeys("nebula", "ip_scene_strategy__visit_incident_count__1h__slot"));


        incidentVarList.add("ip__visit_incident_max_rate__1h__slot");
        incidentVarList.add("ip__visit_incident_count__1h__slot");
        incidentVarList.add("ip__visit_incident_score__1h__slot");
        incidentVarList.add("ip__visit_incident_first_timestamp__1h__slot");
        incidentVarList.add("ip_uid__visit_dynamic_count_top20__1h__slot");
        incidentVarList.add("ip_page__visit_dynamic_count_top20__1h__slot");
        incidentVarList.add("ip_did__visit_dynamic_count_top20__1h__slot");
        incidentVarList.add("ip_tag__visit_incident_count_top20__1h__slot");
        incidentVarList.add("ip_scene_strategy__visit_incident_group_count__1h__slot");
        incidentVarList.add("ip__visit__tag_incident_count__1h__slot");
        incidentVarList.add("ip__visit_dynamic_distinct_count_uid__1h__slot");
    }

    public void start() {
        String babel_mode = CommonDynamicConfig.getInstance().getString("babel_server", "redis");
        Boolean is_redis;
        if (babel_mode.equals("redis")) {
            is_redis = true;
        } else {
            is_redis = false;
        }
        this.incidentBabelSender.start(is_redis);
        logger.info("incident babel sender have started");
    }

    public void stop() {
        logger.info("ZJP.IncidentDataHelper.stop");
        logger.info("incident flush: " + this.flush_count);
        this.incidentBabelSender.stop();
    }

    public void store(final Map dataMap) {
        logger.info("ZJP.IncidentDataHelper.store: " + new Gson().toJson(dataMap));
        if (!dataMap.get("dimension").equals("ip") || !dataMap.containsKey("ip__visit_incident_score__1h__slot")) {
            return;
        }

        IncidentDataObj dataObj = this.format((String) dataMap.get("key"), dataMap);
        if (dataObj == null) {
            return;
        }
        HashMap<String, Object> src = (HashMap<String, Object>) dataObj.getValue();
        HashMap<String, Object> res = new HashMap<>();
        for (String variable : this.incidentVarList) {
            res.put(variable, src.getOrDefault(variable, null));
        }

        Event e = new Event(incidentApp, "incident_add", dataObj.getKey(), System.currentTimeMillis(), 1.0, res);
        incidentBabelSender.send(e);
        logger.debug("output Incident Data event:>>>{}", e);
        SlotMetricsHelper.getInstance().addMetrics("incident.flush", 1.0);
        this.flush_count += 1;
    }

    /**
     * Format a key's statistic cache hashmap to Continuous storage convenient.
     *
     * @param stringObjectHashMap a HashMap which contain variable(String) to their Statistic(Object).
     * @param key                 ip's key for now.
     * @return com.threathunter.nebula.slot.offline.ContinuousDataObj
     **/
    public IncidentDataObj format(String key, Map<String, Object> stringObjectHashMap) {
        IncidentDataObj dataObj = new IncidentDataObj(key, stringObjectHashMap);
        return dataObj;
    }
}
