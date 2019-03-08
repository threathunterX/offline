package com.threathunter.mock.util;

import com.threathunter.common.ObjectId;
import com.threathunter.model.Event;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by daisy on 16-4-8.
 */
public class HttpDynamicEventMaker implements EventMaker {
    int mockCount;
//    protected List<String> properties;

    private String[] commonRandomIps;
    private String[] commonRandomIpcs;
    private String[] commonRandomPage;
    private String[] commonRandomQuery;
    private String[] commonRandomUids;
    private String[] commonRandomDids;
//    private String[] commonRandomLocations;

    private Random r = new Random();

    public HttpDynamicEventMaker() {
        this(10);
    }

    /**
     * Common properties
     */
    public HttpDynamicEventMaker(int mockCount) {
        this.mockCount = mockCount;
        initial();
    }

    private void initial() {
//        properties = new ArrayList<>();
        commonRandomIps = new String[mockCount];
        commonRandomIpcs = new String[mockCount];
        commonRandomPage = new String[mockCount];
        commonRandomQuery = new String[mockCount];
        commonRandomUids = new String[mockCount];
        commonRandomDids = new String[mockCount];
//        commonRandomLocations = new String[mockCount];
        for (int i = 0; i < mockCount; i++) {
            String ipc = generateIpc();
            commonRandomIps[i] = ipc + "." + (r.nextInt(1000000) % 255);
            commonRandomIpcs[i] = ipc;

            commonRandomUids[i] = generateString(15);
            commonRandomDids[i] = generateString(15);

            commonRandomPage[i] = generateUrl();
            commonRandomQuery[i] = generateUrl();
        }
    }

    public void setCommonRandomIps(String[] commonRandomIps, String[] commonRandomIpcs) {
        this.commonRandomIps = commonRandomIps;
        this.commonRandomIpcs = commonRandomIpcs;
    }

    private String generateUrl() {
        return generateString(8);
    }

    @Override
    public Event nextEvent() {
        Event event = new Event("nebula", getEventName(), "");
        event.setId(new ObjectId().toHexString());
        Map<String, Object> properties = new HashMap<>();

        long timestamp = System.currentTimeMillis();
        event.setTimestamp(timestamp);
        properties.putAll(getCommonProperties());

        event.setPropertyValues(properties);
        event.setKey((String) properties.get("c_ip"));
        return event;
    }

    @Override
    public void close() {

    }

    protected Map<String, Object> getCommonProperties() {
        Map<String, Object> common = new HashMap<>();

        int cip_rand = r.nextInt(mockCount);
        int sip_rand = r.nextInt(mockCount);
        int uid_rand = r.nextInt(mockCount);
        int query_rand = r.nextInt(mockCount);
        int page_rand = r.nextInt(mockCount);
        int did_rand = r.nextInt(mockCount);
        common.put("c_ip", commonRandomIps[cip_rand]);
        common.put("sid", generateString(10));
        common.put("uid", commonRandomUids[uid_rand]);
        common.put("did", commonRandomDids[did_rand]);

        String platform = cip_rand % 2 == 0 ? "mobile" : "PC";
        common.put("platform", platform);
        common.put("c_port", generateInt(3000, 10000));
        String c_body = generateString(50);
        common.put("c_body", c_body);
        common.put("c_bytes", c_body.getBytes().length);
        common.put("c_type", "text/html");

        common.put("s_ip", commonRandomIps[sip_rand]);
        common.put("s_port", generateInt(3000, 10000));
        String s_body = generateString(100);
        common.put("s_body", s_body);
        common.put("s_bytes", s_body.getBytes().length);
        common.put("s_type", "text/html");
        common.put("host", generateString(10));

        String query = commonRandomQuery[query_rand];
        String page = "http://" + commonRandomPage[page_rand];
        common.put("page", page);
        common.put("uri_stem", page + query);
        common.put("uri_query", query);
        common.put("referer", "XXX");
        common.put("method", "POST");
        common.put("referer_hit", cip_rand % 2 == 0 ? "T" : "F");
        common.put("status", 200);
        common.put("useragent", "chrome");
        common.put("cookie", generateString(10));
        common.put("xforward", generateString(10));
        common.put("request_type", "mobile");
        common.put("request_time", System.currentTimeMillis());
        return common;
    }

    private String generateIp() {
        StringBuffer str = new StringBuffer();
        str.append(r.nextInt(1000000) % 255);
        str.append(".");
        str.append(r.nextInt(1000000) % 255);
        str.append(".");
        str.append(r.nextInt(1000000) % 255);
        str.append(".");
        str.append(r.nextInt(1000000) % 255);

        return str.toString();
    }

    private String generateIpc() {
        StringBuffer str = new StringBuffer();
        str.append(r.nextInt(1000000) % 255);
        str.append(".");
        str.append(r.nextInt(1000000) % 255);
        str.append(".");
        str.append(r.nextInt(1000000) % 255);

        return str.toString();
    }

    protected String generateString(int length) {
        return RandomStringUtils.randomAlphanumeric(length);
    }

    protected Integer generateInt(int start, int end) {
        return RandomUtils.nextInt(start, end);
    }


    @Override
    public String getEventName() {
        return "HTTP_DYNAMIC";
    }
}
