package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.MetricsHelper;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import com.threathunter.model.PropertyCondition;
import com.threathunter.model.PropertyMapping;
import com.threathunter.model.PropertyReduction;
import com.threathunter.model.VariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by toyld on 4/30/17.
 */
public class ServerMain {
    private static Logger logger = LoggerFactory.getLogger(ServerMain.class);

    public static void main(String[] args) {
        double start_time = System.currentTimeMillis();
        // first public common config: nebula.conf
        CommonDynamicConfig.getInstance().addConfigFile(1, TimeUnit.DAYS, "nebula.conf");
        CommonDynamicConfig.getInstance().addConfigFile(1, TimeUnit.DAYS, "offline.conf");
        CommonDynamicConfig.getInstance().addOverrideProperty("engine.mode.wait", true);
        CommonDynamicConfig.getInstance().addOverrideProperty("metrics_db", "nebula.offline");
        CommonDynamicConfig conf = CommonDynamicConfig.getInstance();
        conf.addOverrideProperty("auth", "40eb336d9af8c9400069270c01e78f76");
        try {
            PropertyCondition.init();
            PropertyMapping.init();
            PropertyReduction.init();
            VariableMeta.init();
            SlotConfigUpdater.getInstance().doUpdates();
        } catch (Exception e) {
            logger.error("init environment error", e);
            return;
        }
        if (args.length > 0) {
            logger.info("输入的参数是:{}", args[args.length - 1]);
        } else {
            logger.info("没有输入参数.");
        }

        MetricsAgent.getInstance().start();
        SlotMetricsHelper.getInstance().setDb("nebula.offline");
        OfflineCronServer server = null;
        try {
            if (args.length == 0) {
                server = new OfflineCronServer();
            } else {
                server = new OfflineCronServer(args[args.length - 1]);
            }
            server.start();
            SlotMetricsHelper.getInstance().addMetrics("cronjob.success", (double) 1);
            SlotMetricsHelper.getInstance().addMetrics("cronjob.costs", (System.currentTimeMillis() - start_time) / 60000);
            // wait metrics to send.
            try {
                logger.warn("waiting metrics to send");
                Thread.sleep(60000);
            } catch (Exception te) {
            }
        } catch (Exception e) {
            SlotMetricsHelper.getInstance().addMetrics("cronjob.success", (double) 0, "error_type", e.getMessage());
            logger.error("error", e);
            try {
                Thread.sleep(60000);
            } catch (Exception te) {
            }
            System.exit(-1);
        } finally {
            if (server != null) {
                server.stop();
            }
            MetricsHelper.getInstance().stop();
        }
        System.exit(0);
    }
}
