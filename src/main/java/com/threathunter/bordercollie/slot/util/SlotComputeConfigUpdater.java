package com.threathunter.bordercollie.slot.util;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.VariableMeta;
import com.threathunter.persistent.core.EventSchemaRegister;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 16/5/30.
 */
public class SlotComputeConfigUpdater {
    private static final Logger logger = LoggerFactory.getLogger(SlotComputeConfigUpdater.class);

    private static final SlotComputeConfigUpdater configUtil = new SlotComputeConfigUpdater();

    private ScheduledExecutorService updateSchedualer = Executors.newSingleThreadScheduledExecutor();

    public static SlotComputeConfigUpdater getInstance() {
        return configUtil;
    }

    public void start() {
        try {
            doUpdates();
            if (EventSchemaRegister.getInstance().getUpdateTimeStamp() < 0) {
                throw new RuntimeException("failed to update events schema and headers");
            }
        } catch (Exception e) {
            logger.error("init:fatal:meet exception while polling", e);
            System.exit(0);
        }
        updateSchedualer.scheduleWithFixedDelay(() -> {
            try {
                doUpdates();
            } catch (Exception e) {
                logger.error("data:fatal:meet exception while polling", e);
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    public void stop() {
        updateSchedualer.shutdown();
        updateSchedualer = null;
    }


    private void doUpdates() throws Exception {
        updateOfflineVariableNodes();
        logger.warn("update offline variable success");
        updateLogSchemaWithVersion();
        logger.warn("update log persistent schema success");
    }

    private void updateLogSchemaWithVersion() throws Exception {
        // first load from online config, if failed, load from local,
        // if EventSchemaRegister has not been update.
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> eventObjects = null;

        String eventurl = CommonDynamicConfig.getInstance().getString("nebula.meta.poller.eventurl");
        if (eventurl != null && !eventurl.isEmpty()) {
            String body = getRestfulResult(eventurl);
            Map<Object, Object> response = mapper.readValue(body, Map.class);
            eventObjects = (List<Map<String, Object>>) response.get("values");
        } else if (EventSchemaRegister.getInstance().getUpdateTimeStamp() < 0) {
            eventObjects = mapper.readValue(Thread.currentThread().getContextClassLoader().getResourceAsStream("events_schema.json"), List.class);
        }

        Map<String, List<String>> versionHeader = null;
        String headerString = CommonDynamicConfig.getInstance().getString("header_version");
        if (headerString != null && !headerString.isEmpty()) {
            versionHeader = mapper.readValue(headerString, Map.class);
        } else if (EventSchemaRegister.getInstance().getUpdateTimeStamp() < 0) {
            versionHeader = mapper.readValue(Thread.currentThread().getContextClassLoader().getResourceAsStream("header_version.json"), Map.class);
        }

//        EventSchemaRegister.getInstance().update(eventObjects, versionHeader);
    }

    private void updateOfflineVariableNodes() throws Exception {
        String offlineVariablesUrl = CommonDynamicConfig.getInstance().getString("nebula.meta.offline.variableurl");
        if (offlineVariablesUrl == null || offlineVariablesUrl.isEmpty()) {
            throw new Exception("offline variables is empty");
        }
        String body = getRestfulResult(offlineVariablesUrl);
        ObjectMapper mapper = new ObjectMapper();
        Map<Object, Object> response = mapper.reader(Map.class).readValue(body);
        List<Object> variableObjects = (List<Object>) response.get("values");
        List<VariableMeta> variableMetas = new ArrayList<>();
        if (variableObjects != null) {
            variableObjects.forEach(o -> {
                VariableMeta meta = VariableMeta.from_json_object(o);
                variableMetas.add(meta);
            });
        }

        SlotVariableMetaRegister.getInstance().update(variableMetas);
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
}
