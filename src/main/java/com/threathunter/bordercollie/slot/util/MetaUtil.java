package com.threathunter.bordercollie.slot.util;

import com.threathunter.common.Identifier;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMetaRegistry;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.VariableMetaBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * 
 */
public class MetaUtil {
    private static Logger logger = LoggerFactory.getLogger(MetaUtil.class);

    public static VariableMeta getMetas(List<VariableMeta> metas, Identifier id) {
        if (metas == null)
            return null;
        return metas.stream().filter(meta -> id.equals(Identifier.fromKeys(meta.getApp(), meta.getName()))).findAny().orElse(null);
    }

    public static List<VariableMeta> getOfflineMetas() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<Object> variableObjects = null;
            try {
                URL in = MetaUtil.class.getClassLoader().getResource("slot_metas.json");
                variableObjects = mapper.reader(List.class).readValue(in);
            } catch (Exception e) {
                e.printStackTrace();
            }
            VariableMetaBuilder builder = new VariableMetaBuilder();
            List<VariableMeta> metas = builder.buildFromJson(variableObjects);
            return metas;
        } catch (Exception e) {
            logger.error("get variable meta for offline error,{}", e);
        }
        return null;
    }

    private static String getRestfulResult(String url) throws Exception {
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

    private static HttpURLConnection getHttpURLConnection(String curEventUrl) throws Exception {
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

    private static String readInputStream(InputStream in) throws IOException {
        char[] buffer = new char[2000];
        StringBuilder result = new StringBuilder();
        InputStreamReader ins = new InputStreamReader(in);
        int readBytes;
        while ((readBytes = ins.read(buffer, 0, 2000)) >= 0) {
            result.append(buffer, 0, readBytes);
        }
        return result.toString();
    }

    public static void initVariablemeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> variableObjects = null;
        try {
            URL in = MetaUtil.class.getClassLoader().getResource("slot_metas.json");
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
