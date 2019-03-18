package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.SlotVariableMetaRegister;
import com.threathunter.bordercollie.slot.util.StrategyInfoCache;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMetaRegistry;
import com.threathunter.model.VariableMeta;
import com.threathunter.persistent.core.EventSchemaRegister;
import com.threathunter.variable.VariableMetaBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class SlotConfigUpdater {
    // todo move to nebula_slot_compute
    private static final Logger logger = LoggerFactory.getLogger(SlotConfigUpdater.class);

    private static final SlotConfigUpdater configUtil = new SlotConfigUpdater();

    public static SlotConfigUpdater getInstance() {
        return configUtil;
    }

    public void doUpdates() throws Exception {
        try {
            if ("dev".equals(CommonDynamicConfig.getInstance().getString("environment"))) {
                initEventMeta();
                logger.warn("update event metas success (json)");
                initVariablemeta();
                logger.warn("update variable metas success (json)");
                initStrategyInfoCache();
                logger.warn("update strategy info cache success (json)");
                return;
            }
            updateEventMetas();
            logger.warn("update event metas success");
            updateOfflineVariableNodes();
            logger.warn("update offline variable success");
            doPollingStrategiesInfo();
            logger.warn("update strategy info success");
        } catch (Exception e) {
            logger.error("error for update");
            throw new RuntimeException("fail to update variable", e);
        }

    }

    private void updateEventMetas() throws Exception {
        String eventMetasUrl = CommonDynamicConfig.getInstance().getString("nebula.meta.poller.eventurl");
        if (eventMetasUrl == null || eventMetasUrl.isEmpty()) {
            throw new Exception("event metas is empty");
        }
        String body = getRestfulResult(eventMetasUrl);
        if (body.isEmpty()) {
            System.out.println(eventMetasUrl);
            throw new Exception("fail to fetch event metas ");
        }
        ObjectMapper mapper = new ObjectMapper();
        Map<Object, Object> response = mapper.readValue(body, Map.class);
        List<Object> eventsObjects = null;
        eventsObjects = (List<Object>) response.get("values");
        eventsObjects.forEach(event -> EventMetaRegistry.getInstance().addEventMeta(BaseEventMeta.from_json_object(event)));
    }

    private void updateOfflineVariableNodes() throws Exception {
        String offlineVariablesUrl = CommonDynamicConfig.getInstance().getString("nebula.meta.offline.variableurl");
        if (offlineVariablesUrl == null || offlineVariablesUrl.isEmpty()) {
            throw new Exception("offline variables is empty");
        }
        String body = getRestfulResult(offlineVariablesUrl);
        if (body.isEmpty()) {
            System.out.println(offlineVariablesUrl);
            throw new Exception("fetch offline variables is empty");
        }
        ObjectMapper mapper = new ObjectMapper();
        Map<Object, Object> response = mapper.reader(Map.class).readValue(body);
        List<Object> variableObjects = (List<Object>) response.get("values");
        List<VariableMeta> variableMetas = new ArrayList<>();
        VariableMetaBuilder builder = new VariableMetaBuilder();
        List<VariableMeta> metas = builder.buildFromJson(variableObjects);
        logger.info("init in common base: metas size = {}", metas.size());
        SlotVariableMetaRegister.getInstance().update(metas);
    }

    private void doPollingStrategiesInfo() throws Exception {
        String strategyInfoUrl = CommonDynamicConfig.getInstance().getString("nebula.meta.strategyurl");
        if (strategyInfoUrl == null || strategyInfoUrl.isEmpty()) {
            throw new Exception("strategy information url is empty");
        }
        String body = getRestfulResult(strategyInfoUrl);
        ObjectMapper mapper = new ObjectMapper();
        Map<Object, Object> response = mapper.reader(Map.class).readValue(body);
        List<Map<String, Object>> strategyObjects = (List<Map<String, Object>>) response.get("values");

        StrategyInfoCache.getInstance().update(strategyObjects);
    }

    private String getRestfulResult(String url) throws Exception {
        InputStream inputStream = null;
        try {
            String authUrl;
            if (!url.contains("?")) {
                authUrl = String.format("%s?auth=%s", url, CommonDynamicConfig.getInstance().getString("auth"));
            } else {
                authUrl = String.format("%s&auth=%s", url, CommonDynamicConfig.getInstance().getString("auth"));
            }
            HttpURLConnection conn = getHttpURLConnection(authUrl);
            inputStream = conn.getInputStream();
            return readInputStream(inputStream);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private HttpURLConnection getHttpURLConnection(String curEventUrl) throws Exception {
        URL u = new URL(curEventUrl);
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(1000 * 30);
        conn.setRequestMethod("GET");
        conn.setInstanceFollowRedirects(false);
        conn.setRequestProperty("accept", "*/*");
        conn.setRequestProperty("connection", "Keep-Alive");
        conn.setRequestProperty("content-type", "application/json");
        conn.setDoOutput(false);
        conn.setDoInput(true);
        return conn;
    }

    private String readInputStream(InputStream in) throws IOException {
        char[] buffer = new char[2000];
        StringBuilder result = new StringBuilder();
        InputStreamReader ins = new InputStreamReader(in);
        int readBytes;
        while ((readBytes = ins.read(buffer, 0, 2000)) >= 0) {
            result.append(buffer, 0, readBytes);
        }
        return result.toString();
    }

    private void initStrategyInfoCache() {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> strategyObjects;
        try {
            URL in = SlotConfigUpdater.class.getClassLoader().getResource("strategy.json");
            strategyObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            throw new RuntimeException("strategy cache init", e);
        }
        StrategyInfoCache.getInstance().update(strategyObjects);
    }

    private static void initEventMeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> eventsObjects = null;
        try {
            URL in = SlotConfigUpdater.class.getClassLoader().getResource("events.json");
            eventsObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        eventsObjects.forEach(event -> EventMetaRegistry.getInstance().addEventMeta(BaseEventMeta.from_json_object(event)));
    }

    private static void initVariablemeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> variableObjects = null;
        try {
            URL in = SlotConfigUpdater.class.getClassLoader().getResource("slot_metas.json");
            variableObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        VariableMetaBuilder builder = new VariableMetaBuilder();
        List<VariableMeta> metas = builder.buildFromJson(variableObjects);
//        List<VariableMeta> metas = builder.buildFromJson(JsonFileReader.getValuesFromFile("slot_metas.json", JsonFileReader.ClassType.LIST));
        logger.info("init in common base: metas size = {}", metas.size());
        SlotVariableMetaRegister.getInstance().update(metas);
    }
}
