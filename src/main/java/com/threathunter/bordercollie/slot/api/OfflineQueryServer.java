package com.threathunter.bordercollie.slot.api;

import com.threathunter.babel.rpc.ServiceContainer;
import com.threathunter.babel.rpc.impl.ServerContainerImpl;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by toyld on 4/6/17.
 */
public class OfflineQueryServer {
    private static final Logger logger = LoggerFactory.getLogger(OfflineQueryServer.class);

    private volatile boolean running = false;
    private volatile ServiceContainer queryServer = null;

    public OfflineQueryServer() {

    }

    public void start() {
        init();
        boolean redisMode = false;
        if ("redis".equals(CommonDynamicConfig.getInstance().getString("babel_server"))) {
            redisMode = true;
        }

        logger.warn("start:start variable query server");
        queryServer = new ServerContainerImpl();
        queryServer.addService(new ContinuousQueryService(redisMode));
        queryServer.addService(new OfflineSlotQueryService(redisMode));
        queryServer.addService(new OfflineMergeQueryService(redisMode));
        //  queryServer.addService(new OfflineBaselineQueryService(redisMode));
        queryServer.start();

        logger.warn("start:nebula is started successfully, enjoy!");
        running = true;
        while (running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        logger.warn("close:finish nebula processing");
    }

    public void init() {
        logger.warn("start:init configuration");

        CommonDynamicConfig conf = CommonDynamicConfig.getInstance();
        // auth
        conf.addOverrideProperty("auth", "40eb336d9af8c9400069270c01e78f76");

        logger.warn("start:init redis related config");
        MetricsAgent.getInstance().start();
    }

    public boolean isRunning() {
        return this.running;
    }

    public void stop() {
        running = false;
        logger.warn("close:stopping nebula success");
    }
}
