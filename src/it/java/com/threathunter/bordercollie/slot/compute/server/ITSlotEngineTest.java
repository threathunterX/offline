package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.bordercollie.slot.compute.SlotFactory;
import com.threathunter.bordercollie.slot.compute.SlotQueryable;
import com.threathunter.common.Identifier;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by yy on 17-11-7.
 */
public class ITSlotEngineTest extends ITCommonBaseWithWindowChange {
    //V1:uid__visit_dynamic_count__1h__slot
    @Test
    @Ignore
    public void testV1() throws InterruptedException {
        Thread.sleep(1000);
        SlotQueryable query = SlotFactory.createSlotQueryable(engine);
        //Map<Long, Object> variable = query.queryAllCommon(Identifier.fromKeys("nebula","ip__visit__did_dynamic_count__1h__slot"),"127.0.0.1");
        Object obj = query.queryCurrent(Identifier.fromKeys("nebula", "uid__visit_dynamic_count__1h__slot"), Arrays.asList("345736470"));
        assertThat(obj).isNotNull();
    }

    //V2:uid_geo_province__visit_dynamic_count_top20__1h__slot
    @Test
    @Ignore
    public void testv2() throws InterruptedException {
        SlotQueryable query = SlotFactory.createSlotQueryable(engine);
        Object obj = query.queryCurrent(Identifier.fromKeys("nebula", "uid_geo_city__visit_dynamic_count_top20__1h__slot"), Arrays.asList("345736470"));
        assertThat(obj).isNotNull();
    }

    //v3:uid_useragent__visit_dynamic_count_top20__1h__slot
    @Test
    @Ignore
    public void testv3() throws InterruptedException {
        SlotQueryable query = SlotFactory.createSlotQueryable(engine);
        Object obj = query.queryCurrent(Identifier.fromKeys("nebula", "uid_useragent__visit_dynamic_count_top20__1h__slot"), Arrays.asList("345736470"));
        assertThat(obj).isNotNull();
    }

    //v4:uid_did__visit_dynamic_count_top20__1h__slot
    @Test
    @Ignore
    public void testv4() throws InterruptedException {
        SlotQueryable query = SlotFactory.createSlotQueryable(engine);
        Object obj = query.queryCurrent(Identifier.fromKeys("nebula", "uid_did__visit_dynamic_count_top20__1h__slot"), Arrays.asList("345736470"));
        assertThat(obj).isNotNull();
    }

    @Test
    public void testPreviousV1() throws InterruptedException {
        setUp();
//        Thread.sleep(70000);
        SlotQueryable query = SlotFactory.createSlotQueryable(engine);
        //Map<Long, Object> variable = query.queryAllCommon(Identifier.fromKeys("nebula","ip__visit__did_dynamic_count__1h__slot"),"127.0.0.1");
        Object obj = query.queryPrevious(Identifier.fromKeys("nebula", "uid__visit_dynamic_count__1h__slot"), Arrays.asList("345736470"));
        assertThat(obj).isNotNull();
    }

}
