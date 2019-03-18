package com.threathunter.bordercollie.slot.compute.client;

import com.threathunter.bordercollie.slot.api.OfflineSlotDataHelper;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * 
 */
public class ITLeveDBApiTest {
    @Test
    public void queryLeveldb() {
        Timestamp t = new Timestamp(1511589600000L);
        System.out.println(t.getTime());
//        Timestamp t=new Timestamp(1511589600*1000);
        OfflineSlotDataHelper offlineSlotDataHelper = new OfflineSlotDataHelper(t.getTime(), true);
        List<String> varList = new ArrayList<String>();
        varList.add("ip__visit_dynamic_distinct_count_uid__1h__slot");

        Map<String, Object> result = offlineSlotDataHelper.getStatistic("172.16.10.110", "ip", varList);
        assertThat(result).isNotNull();
    }

}
