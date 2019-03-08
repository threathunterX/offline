package com.threathunter.bordercollie.slot.util;

import com.threathunter.mock.simulator.EventParser;
import com.threathunter.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 1.5
 */
@Slf4j
public class EventParserTest {
    String jsonStr;

    @Before
    public void setUp() {
        // jsonStr="{app='nebula', name='offline_merge_variablequery_request', key='__GLOBAL__', id='5a3a111b66b43e5d703d799b', pid='000000000000000000000000', value=0.0, timestamp=1513754907340, propertyValues={app=nebula, keys=['www.strategy_test.com/province/2/city/1', 'www.so.com/test/province/****', www.so.com/test/province/****/****/4, www.so.com/test/province/4/****/4, www.so.com/province/21, www.so.com/test/c/province/5/city/****/details, www.so.com/province/20, www.strategy_test.com/province/21, www.strategy_test.com/province/20], time_list=[1513699200000, 1513702800000, 1513706400000, 1513710000000, 1513713600000, 1513717200000, 1513720800000, 1513724400000, 1513728000000, 1513731600000, 1513735200000, 1513738800000, 1513742400000, 1513746000000, 1513749600000], var_list=[page__visit_incident_count__1h__slot, page__visit_dynamic_distinct_count_ip__1h__slot, page_ip__visit_dynamic_count_top100__1h__slot, page__visit_dynamic_distinct_count_uid__1h__slot, page_uid__visit_dynamic_count_top100__1h__slot,page__visit_dynamic_distinct_count_did__1h__slot, page_did__visit_dynamic_count_top100__1h__slot], dimension=page}}";
        jsonStr = "{app='nebula', name='offline_merge_variablequery_request', key='__GLOBAL__', id='5a3a111b66b43e5d703d799b', pid='000000000000000000000000', value=0.0, timestamp=1513754907340, propertyValues={app=nebula, keys=['www.strategy_test.com/province/2/city/1', 'www.so.com/test/province/****'], time_list=[1513699200000, 1513702800000, 1513706400000, 1513710000000, 1513713600000, 1513717200000, 1513720800000, 1513724400000, 1513728000000, 1513731600000, 1513735200000, 1513738800000, 1513742400000, 1513746000000, 1513749600000], var_list=[page__visit_incident_count__1h__slot, page__visit_dynamic_distinct_count_ip__1h__slot, page_ip__visit_dynamic_count_top100__1h__slot, page__visit_dynamic_distinct_count_uid__1h__slot, page_uid__visit_dynamic_count_top100__1h__slot,page__visit_dynamic_distinct_count_did__1h__slot, page_did__visit_dynamic_count_top100__1h__slot], dimension=page}}";
        // {app='nebula', name='offline_merge_variablequery_request', key='__GLOBAL__', id='5a3a111b66b43e5d703d799b', pid='000000000000000000000000', value=0.0, timestamp=1513754907340, propertyValues={app=nebula, keys=[www.strategy_test.com/province/2/city/1, www.so.com/test/province/****, www.so.com/test/province/****/****/4, www.so.com/test/province/4/****/4, www.so.com/province/21, www.so.com/test/c/province/5/city/****/details, www.so.com/province/20, www.strategy_test.com/province/21, www.strategy_test.com/province/20], time_list=[1513699200000, 1513702800000, 1513706400000, 1513710000000, 1513713600000, 1513717200000, 1513720800000, 1513724400000, 1513728000000, 1513731600000, 1513735200000, 1513738800000, 1513742400000, 1513746000000, 1513749600000], var_list=[page__visit_incident_count__1h__slot, page__visit_dynamic_distinct_count_ip__1h__slot, page_ip__visit_dynamic_count_top100__1h__slot, page__visit_dynamic_distinct_count_uid__1h__slot, page_uid__visit_dynamic_count_top100__1h__slot,page__visit_dynamic_distinct_count_did__1h__slot, page_did__visit_dynamic_count_top100__1h__slot], dimension=page}}
//        jsonStr="{app='nebula', name='offline_merge_variablequery_request', key='__GLOBAL__', id='5a3a111b66b43e5d703d799b', pid='000000000000000000000000', value=0.0, timestamp=1513754907340}";

    }

    @Test
    public void testFromJsonString() {
        Event event = EventParser.fromJsonString(jsonStr);
        Map<String, Object> propertyValues = event.getPropertyValues();
        Object keys = propertyValues.get("keys");
        List<String> ss = (List) keys;
        List<String> newList = new ArrayList<>();
        for (String str : ss) {
            String s = str.replaceAll("\\^", "\\/");
            newList.add(s);
        }
        propertyValues.put("keys", newList);
        event.setPropertyValues(propertyValues);
        assertThat(event).isNotNull();
        log.info("event : {}", event);
    }
}
