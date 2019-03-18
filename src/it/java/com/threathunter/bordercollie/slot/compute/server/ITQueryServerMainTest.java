package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.bordercollie.slot.api.QueryServerMain;
import com.threathunter.config.CommonDynamicConfig;
import org.junit.Test;

/**
 * 
 */
public class ITQueryServerMainTest {
    @Test
    public void testQuery() {
        CommonDynamicConfig.getInstance().addOverrideProperty("persist_path", System.getProperty("user.dir") + "/persistent");
        CommonDynamicConfig.getInstance().addConfigFile("babel.conf");
        QueryServerMain.main(new String[]{});
    }
}
