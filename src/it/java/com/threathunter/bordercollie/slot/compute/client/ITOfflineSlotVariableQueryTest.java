package com.threathunter.bordercollie.slot.compute.client;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.RemoteException;
import com.threathunter.babel.rpc.ServiceClient;
import com.threathunter.babel.rpc.impl.ServiceClientImpl;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.google.gson.Gson;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17-12-10
 */
public class ITOfflineSlotVariableQueryTest {
    @Test
    public void testOfflineQuery() throws RemoteException {
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");
        CommonDynamicConfig.getInstance().addOverrideProperty("redis_host", "127.0.0.1");
        CommonDynamicConfig.getInstance().addOverrideProperty("redis_port", 6379);
        ServiceMeta meta = ServiceMetaUtil.getMetaFromResourceFile("LicenseInfo_redis.service");
        // dhtgGZEZ5Foi6if
        ServiceClient client = new ServiceClientImpl(meta);
        client.start();
        Event request = new Event("nebula", "licenseinfo", "");
        request.setTimestamp(System.currentTimeMillis());
        Map<String, Object> properties = Collections.EMPTY_MAP;
 /*       properties.put("keys", Arrays.asList("dhtgGZEZ5Foi6if"));
//        properties.put("keys", Arrays.asList("__GLOBAL__"));
        properties.put("var_list", Arrays.asList("uid_page__visit_dynamic_count_top20__1h__slot"));
//        properties.put("var_list", Arrays.asList("uid__visit_dynamic_count_top100__1h__slot"));
        properties.put("timestamp", 1512885600000l);
//        properties.put("dimension", "global");
        properties.put("dimension", "uid");
        request.setPropertyValues(properties);*/

        Event response = client.rpc(request, meta.getName(), 10, TimeUnit.SECONDS);
        System.out.println(new Gson().toJson(response));
        client.stop();
    }
}
