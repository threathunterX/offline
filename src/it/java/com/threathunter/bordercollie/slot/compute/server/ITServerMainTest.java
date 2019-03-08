package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.bordercollie.slot.EnvironmentUtil;
import com.threathunter.bordercollie.slot.api.OfflineCronServer;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import com.threathunter.variable.DimensionType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 17-11-22.
 */
public class ITServerMainTest {
    public static Logger logger = LoggerFactory.getLogger(ITServerMainTest.class);

    @Test
    public void testCronManager() throws FileNotFoundException {
        long start_time = System.currentTimeMillis();
        // first public common config: nebula.conf
        CommonDynamicConfig.getInstance().addOverrideProperty("persist_path", System.getProperty("user.dir") + "/persistent");
        CommonDynamicConfig.getInstance().addOverrideProperty("enable_continuous_output", true);
        CommonDynamicConfig.getInstance().addOverrideProperty("enable_incident_output", true);
        CommonDynamicConfig.getInstance().addConfigFile(1, TimeUnit.DAYS, "nebula.conf");
        CommonDynamicConfig.getInstance().addConfigFile(1, TimeUnit.DAYS, "offline.conf");
        CommonDynamicConfig.getInstance().addConfigFile(1, TimeUnit.DAYS, "babel.conf");
        CommonDynamicConfig.getInstance().addOverrideProperty("metrics_db", "nebula.offline");
//        CommonDynamicConfig.getInstance().addOverrideProperty("slot_dimensions", "ip|glob");
        CommonDynamicConfig conf = CommonDynamicConfig.getInstance();
        conf.addOverrideProperty("auth", "40eb336d9af8c9400069270c01e78f76");
        MetricsAgent.getInstance().start();
        List<DimensionType> dimensionTypes = new ArrayList<DimensionType>();

        dimensionTypes.add(DimensionType.IP);
        dimensionTypes.add(DimensionType.DID);
        dimensionTypes.add(DimensionType.UID);
        dimensionTypes.add(DimensionType.GLOBAL);
        dimensionTypes.add(DimensionType.PAGE);
        dimensionTypes.add(DimensionType.OTHER);
        EnvironmentUtil.initAll();
     /*   PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        EnvironmentUtil.initEventMeta();
        EnvironmentUtil.initVariablemeta();
        EnvironmentUtil.initStrategyInfoCache();*/


        OfflineCronServer manager = new OfflineCronServer("2017122014");
        manager.start();
        long endTime = System.currentTimeMillis();
        long elapse = endTime - start_time;

        logger.info("===========engine stop===================");
        logger.error("====elapsed time===={}", elapse);
        while (true) ;
    }

}
