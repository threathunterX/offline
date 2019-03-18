package com.threathunter.bordercollie.slot.compute.client;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.RemoteException;
import com.threathunter.babel.rpc.ServiceClient;
import com.threathunter.babel.rpc.ServiceContainer;
import com.threathunter.babel.rpc.impl.ServiceClientImpl;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import com.threathunter.mock.simulator.EventParser;
import com.threathunter.model.Event;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.fail;

/**
 * 
 */
public class ITBabelClientTest {
    private static ServiceClient client;
    private static ServiceContainer server;
    private static String eventStr;

    @BeforeClass
    public static void setUp() {
        CommonDynamicConfig.getInstance().addOverrideProperty("metrics_server", "redis");
        MetricsAgent.getInstance().start();
        eventStr = "{app='nebula', name='offline_merge_variablequery_request', key='__GLOBAL__', id='5a3a111b66b43e5d703d799b', pid='000000000000000000000000', value=0.0, timestamp=1513754907340, propertyValues={app=nebula, keys=['www.strategy_test.com/province/2/city/1', 'www.so.com/test/province/****', 'www.so.com/test/province/****/****/4', 'www.so.com/test/province/4/****/4', 'www.so.com/province/21', 'www.so.com/test/c/province/5/city/****/details', 'www.so.com/province/20', 'www.strategy_test.com/province/21', 'www.strategy_test.com/province/20'], time_list=[1513699200000, 1513702800000, 1513706400000, 1513710000000, 1513713600000, 1513717200000, 1513720800000, 1513724400000, 1513728000000, 1513731600000, 1513735200000, 1513738800000, 1513742400000, 1513746000000, 1513749600000], var_list=[page__visit_incident_count__1h__slot, page__visit_dynamic_distinct_count_ip__1h__slot, page_ip__visit_dynamic_count_top100__1h__slot, page__visit_dynamic_distinct_count_uid__1h__slot, page_uid__visit_dynamic_count_top100__1h__slot,page__visit_dynamic_distinct_count_did__1h__slot, page_did__visit_dynamic_count_top100__1h__slot], dimension=page}}";
    }

    @BeforeClass
    public static void initial() {
        CommonDynamicConfig.getInstance().addConfigFile("babel.conf");
    }

    @Test
    public void testContinuousQuery() throws RemoteException {
        ServiceMeta meta = ServiceMetaUtil.getMetaFromResourceFile("offline_continuousquery_redis.service");
        ServiceClient client = new ServiceClientImpl(meta);
        client.start();
        Event result = client.rpc(asContinuousEvent(), meta.getName(), 1, TimeUnit.SECONDS);
        if (result == null) {
            fail();
        }
        System.out.println(result);
    }

    private Event asContinuousEvent() {
        Event result = new Event();
        result.setApp("nebula");
        result.setName("offline_continuousquery");
        result.setKey("key");
        result.setValue(7.0);
        result.setTimestamp(System.currentTimeMillis());
        Map<String, Object> properties = new HashMap<>();
        properties.put("key", "172.16.10.128_1511539200");
        properties.put("dimension", "GLOBAL");
        List<String> timeStamps = new ArrayList<String>();
        timeStamps.add("1511589600.0");
        timeStamps.add("1512478800.0");
//        timeStamps.add("")
        properties.put("timestamps", timeStamps);
        List<String> varList = new ArrayList<String>();
        varList.add("ip__visit_dynamic_distinct_count_uid__1h__slot");
        properties.put("var_list", varList);
//        properties.put("name","logquery");
        result.setPropertyValues(properties);

        return result;
//        return null;
    }


    @Test
    public void testMergeQuery() throws RemoteException {
        ServiceMeta meta = ServiceMetaUtil.getMetaFromResourceFile("offline_merged_variable_query_redis.service");
        ServiceClient client = new ServiceClientImpl(meta);
        client.start();
        Event result = client.rpc(EventParser.getEventFromString(eventStr), meta.getName(), 100000000, TimeUnit.SECONDS);
        if (result == null) {
            fail();
        }
        System.out.println(result);
    }

    private Event asMergeEvent() {
        Event result = new Event();
        result.setApp("nebula");
        result.setName("offline_merge_variablequery");
        result.setKey("key");
        result.setValue(7.0);
        result.setTimestamp(System.currentTimeMillis());
        Map<String, Object> properties = new HashMap<>();
        List<String> keys = new ArrayList<>();
        keys.add("www.strategy_test.com/province/2/city/1");
        properties.put("keys", keys);
        properties.put("dimension", "page");
        List<Long> timeStamps = new ArrayList<Long>();
        timeStamps.add(1513587600000L);
        timeStamps.add(1513591200000L);
        properties.put("time_list", timeStamps);
        List<String> varList = new ArrayList<String>();
        varList.add("page__visit_dynamic_count__1h__slot");
        varList.add("page__visit_incident_count__1h__slot");
        properties.put("var_list", varList);
//        properties.put("name","logquery");
        result.setPropertyValues(properties);

        return result;
//        return null;
    }

    private Event asMergeEvent2() {
        Event result = new Event();
        result.setApp("nebula");
        result.setName("offline_merge_variablequery");
        result.setKey("key");
        result.setValue(7.0);
        result.setTimestamp(System.currentTimeMillis());
        Map<String, Object> properties = new HashMap<>();
        List<String> keys = new ArrayList<>();
        keys.add("__GLOBAL__");
        properties.put("keys", keys);
        properties.put("dimension", "global");
        List<Long> timeStamps = new ArrayList<Long>();
        timeStamps.add(1512968400000L);
        timeStamps.add(1512972000000L);
        properties.put("time_list", timeStamps);
        List<String> varList = new ArrayList<String>();
        varList.add("ip__visit_dynamic_count_top100__1h__slot");
        varList.add("ip__visit_dynamic_distinct_count_uid_top100__1h__slot");
        properties.put("var_list", varList);
//        properties.put("name","logquery");
        result.setPropertyValues(properties);

        return result;
//        return null;
    }

    private Event asMergeEvent3() {
        Event result = new Event();
        result.setApp("nebula");
        result.setName("offline_merge_variablequery");
        result.setKey("key");
        result.setValue(7.0);
        result.setTimestamp(System.currentTimeMillis());
        Map<String, Object> properties = new HashMap<>();
        List<String> keys = new ArrayList<>();
        keys.add("uid_000001");
        keys.add("uid_000002");
        properties.put("keys", keys);
        properties.put("dimension", "uid");
        List<Long> timeStamps = new ArrayList<Long>();
        timeStamps.add(1512979200000L);
        timeStamps.add(1512982800000L);
        properties.put("time_list", timeStamps);
        List<String> varList = new ArrayList<String>();
        varList.add("uid_page__visit_dynamic_count_top20__1h__slot");
        varList.add("uid_useragent__visit_dynamic_count_top20__1h__slot");
        properties.put("var_list", varList);
//        properties.put("name","logquery");
        result.setPropertyValues(properties);

        return result;
//        return null;
    }


    //{"keys":["www.strategy_test.com/province/2/city/1"],"timestamp":1513753200000,"dimension":"page","variables":["page_ip__visit_dynamic_count_top100__1h__slot","page_uid__visit_dynamic_count_top100__1h__slot","page_did__visit_dynamic_count_top100__1h__slot"]}
    @Test
    public void testKeyStatQuery() throws RemoteException {
        ServiceMeta meta = ServiceMetaUtil.getMetaFromResourceFile("offline_keystatquery_redis.service");
        ServiceClient client = new ServiceClientImpl(meta);
        client.start();
        Event result = client.rpc(asKeyStatEvent(), meta.getName(), 1000000, TimeUnit.SECONDS);
        if (result == null) {
            fail();
        }
        System.out.println(result);
    }

    private Event asKeyStatEvent() {
        Event result = new Event();
        result.setApp("nebula");
        result.setName("offline_keystatquery");
        result.setKey("key");
        result.setValue(7.0);
        result.setTimestamp(System.currentTimeMillis());
        Map<String, Object> properties = new HashMap<>();
        List<String> keys = new ArrayList<>();
        keys.add("www.strategy_test.com/province/2/city/1");
//        keys.add("127.0.0.2");
        properties.put("keys", keys);
        properties.put("dimension", "page");
//        timeStamps.add("")
        properties.put("timestamp", 1513753200000L);
        List<String> varList = new ArrayList<String>();
        varList.add("page_ip__visit_dynamic_count_top100__1h__slot");
        varList.add("page_uid__visit_dynamic_count_top100__1h__slot");
        varList.add("page_did__visit_dynamic_count_top100__1h__slot");
        //  "page_ip__visit_dynamic_count_top100__1h__slot","page_uid__visit_dynamic_count_top100__1h__slot","page_did__visit_dynamic_count_top100__1h__slot"
        properties.put("var_list", varList);
//        properties.put("name","logquery");
        result.setPropertyValues(properties);

        return result;
    }

}
