package com.threathunter.bordercollie.slot.compute;

import com.threathunter.bordercollie.slot.api.QueryServerMain;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import org.junit.BeforeClass;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;


/**
 * 
 */


@Slf4j
public class ITQueryServerClientTest {


    @BeforeClass
    public static void setUp() {
        log.info("query server main started");
        CommonDynamicConfig.getInstance().addOverrideProperty("persist_path", System.getProperty("user.dir") + "/persistent");
        CommonDynamicConfig.getInstance().addConfigFile("babel.conf");
        CommonDynamicConfig.getInstance().addOverrideProperty("metrics_server", "redis");
        MetricsAgent.getInstance().start();
        QueryServerMain.main(new String[]{});
    }

    @Test
    public void test() {
        System.out.println("123");
    }

}
