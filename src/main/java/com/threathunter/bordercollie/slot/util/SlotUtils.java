package com.threathunter.bordercollie.slot.util;

import com.threathunter.config.CommonDynamicConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.threathunter.persistent.core.util.BytesEncoderDecoder.encode8;


/**
 * 
 */
public class SlotUtils {
    private static final Logger logger = LoggerFactory.getLogger(SlotUtils.class);
    private static final String IP_ADDRESS = "(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})";
    private static final Pattern addressPattern = Pattern.compile(IP_ADDRESS);
    private static Map<String, byte[]> StatKeyMap = new HashMap<>();
    private static CommonDynamicConfig config = CommonDynamicConfig.getInstance();
    public static String slotTimestampFormat = "yyyyMMddHH";
    public static String slotTimestampFormat4M = "yyyyMMddHHmm";
    public static String totalKey = "__GLOBAL__";
    public static List<String> incidentScene = new ArrayList<>(6);

    static {
        incidentScene.add("ACCOUNT");
        incidentScene.add("TRANSACTION");
        incidentScene.add("VISITOR");
        incidentScene.add("OTHER");
        incidentScene.add("MARKETING");
        incidentScene.add("ORDER");
    }

    public SlotUtils() {
        StatKeyMap.put("ip", encode8(2));
        StatKeyMap.put("did", encode8(4));
        StatKeyMap.put("uid", encode8(5));
        StatKeyMap.put("page", encode8(6));
        StatKeyMap.put("global", encode8(7));
        StatKeyMap.put("other", encode8(8));
    }

    public static Map<String, Object> mergeMap(Map<String, Object> srcMap, Map<String, Object> dstMap) {
        if (srcMap.isEmpty() || srcMap == null) {
            return dstMap;
        }
        srcMap.forEach((k, v) -> {
            if (dstMap.containsKey(k)) {
                if (v instanceof Number) {
                    dstMap.put(k, ((Number) v).longValue() + ((Number) dstMap.get(k)).longValue());
                } else if (v instanceof Set) {
                    Set s = (Set) dstMap.get(k);
                    s.addAll((Set) v);
                } else if (v instanceof Map) {
                    Map submergeMap = mergeMap((Map<String, Object>) v, (Map<String, Object>) dstMap.get(k));
                    dstMap.put(k, submergeMap);
                }
            } else {
                dstMap.put(k, v);
            }
        });
        return dstMap;
    }

    public static String getSlotTimestampFormat() {
        return slotTimestampFormat;
    }

    public static String getWorkingHourTSFromStr(String workinghour) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(slotTimestampFormat);
            Date parsedDate = dateFormat.parse(workinghour);
            long tmp = parsedDate.getTime() / 1000;
            return String.valueOf(tmp) + ".0";
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return "";
        }
    }

    public static String getWorkingMinuteTSFromStr(String workingMinute) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(slotTimestampFormat4M);
            Date parsedDate = dateFormat.parse(workingMinute);
            long tmp = parsedDate.getTime() / 1000;
            return String.valueOf(tmp) + ".0";
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return "";
        }
    }

    public static String getTimestampForStartOfDay(String timestamp) {
        Integer ts = Integer.parseInt(timestamp) / (3600 * 24) * 3600 * 24 - 8 * 3600;
        return ts.toString();
    }

    public String getLastHourStr(String format) {
        int jetlag = 3600000; // one hour
        if (format == null) {
            return new DateTime(System.currentTimeMillis() - jetlag).toString(slotTimestampFormat);
        } else {
            return new DateTime(System.currentTimeMillis() - jetlag).toString(format);
        }

    }

    public String getLastM5(String format) {
        int jetlag = 300000;
        if (format == null) {
            return new DateTime(System.currentTimeMillis() - jetlag).toString(slotTimestampFormat);
        } else {
            return new DateTime(System.currentTimeMillis() - jetlag).toString(format);
        }
    }

    public String getLastM5() {
        return getLastM5(null);
    }


    public String getLastHourStr() {
        return getLastHourStr(null);
    }

    public byte[] getIPByte(String ip) {
        byte[] b = new byte[4];
        Matcher matcher = addressPattern.matcher(ip);
        if (matcher.matches()) {
            for (int i = 0; i < 4; ++i) {
                b[i] = (byte) Integer.parseInt(matcher.group(i + 1));
            }
        } else {
            throw new IllegalArgumentException("Could not parse [" + ip + "]");
        }
        return b;
    }

    public byte[] get_stat_key(String key, String dimension) {
        logger.trace(">>>>>levelDB:key:{} dimension:{}", key, dimension);
        byte[] db_suffix;
        byte[] db_key;
        if (!StatKeyMap.containsKey(dimension)) {
            return null;
        }
        byte[] db_prefix = StatKeyMap.get(dimension);
        try {
            if (dimension.equals("ip") && !key.equals(this.totalKey)) {
                db_suffix = getIPByte(key);
            } else {
                db_suffix = key.getBytes();
            }
            // byte[] merge..
            db_key = new byte[db_prefix.length + db_suffix.length];
            System.arraycopy(db_prefix, 0, db_key, 0, db_prefix.length);
            System.arraycopy(db_suffix, 0, db_key, db_prefix.length, db_suffix.length);
        } catch (Exception e) {
//            e.printStackTrace();
            logger.error("could not parse key", e);
            return null;
        }
        return db_key;
    }

    public void CleanWebCache() throws Exception {
        String web_ip = config.getString("webui_address");
        String web_port = config.getString("webui_port");
        String WebCacheUrl = String.format("http://%s:%s/platform/stats/clean_cache?&url=/platform/stats/offline_serial&method=GET", web_ip, web_port);
        String body = getRestfulResult(WebCacheUrl);
        ObjectMapper mapper = new ObjectMapper();
        Map<Object, Object> response = mapper.reader(Map.class).readValue(body);
        Integer resStatus = (Integer) response.get("status");
        if (resStatus != 0) {
            throw new Exception((String) response.get("error"));
        }
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

    public static void main(String[] args) {
        Integer a = Integer.parseInt("1492077600");
        Integer ts = (a / (3600 * 24)) * 3600 * 24 - 8 * 3600;
//        System.out.print(ts.toString());
        System.out.println(getWorkingHourTSFromStr("2017050215"));
        System.exit(0);
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(slotTimestampFormat);
            Date parsedDate = dateFormat.parse("2017050215");
            long c = parsedDate.getTime();
            double d = c / 1000.0;
            System.out.println((parsedDate.getTime() / 1000.0));
        } catch (Exception e) {
            e.printStackTrace();
        }
        // todo 过滤一个字典到此为止了?
//        Map<String, Integer> input = new HashMap<String, Integer>();
//        input.put("var_name1", 2);
//        input.put("var_name2", 3);
//        HashSet<String> vars = new HashSet<>();
//        vars.add("var_name1");
//        Map<String, Integer> output = new HashMap<>();
//        input.keySet().stream().filter( e -> vars.contains(e)).forEach( e -> {output.put(e, input.get(e));});
//        //input.entrySet().stream().filter( e -> { return vars.contains(e.getKey());}).map( e -> output.put(e.getKey(), e.getValue()));
//        //System.out.println(input.entrySet().stream().filter( e -> { return vars.contains(e.getKey());}).toArray());
//        System.out.println(input);
//        System.out.println(output);
//        input.forEach( (e,i) -> {
//            if (!vars.contains(e)) {
//                input.remove(e);
//            }});
//        System.out.println(input);
//        System.out.println(new DateTime(System.currentTimeMillis()).toString("yyyyMMddHH"));
//        System.out.println(new DateTime(System.currentTimeMillis()-3600000).toString("yyyyMMddHH"));
    }
}
