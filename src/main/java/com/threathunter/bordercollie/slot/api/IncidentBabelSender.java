package com.threathunter.bordercollie.slot.api;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.impl.ServiceClientImpl;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 
 */
public class IncidentBabelSender extends BabelSender {
    private static final Logger logger = LoggerFactory.getLogger(IncidentBabelSender.class);
    private final Worker worker;
    private volatile boolean running = false;
    private static final Integer SendIncidentLimit = 500;  // send 500 incident max per time
    protected final BlockingQueue<Event> cache;
    protected ServiceClientImpl client;
    protected ServiceMeta meta;
    protected String goods;


    public IncidentBabelSender() {
        goods = "incident";
        worker = new Worker();
        this.cache = new LinkedBlockingQueue<>();
//        ShutdownHookManager.get().addShutdownHook(() -> stop(), 2);
    }

    public static IncidentBabelSender getInstance() {
        return (IncidentBabelSender) getInstance("com.threathunter.bordercollie.slot.api.IncidentBabelSender");
    }

    public void start(boolean redisMode) {
        String ModeMetaName;
        if (redisMode) {
            ModeMetaName = "incident_notify_redis.service";
        } else {
            ModeMetaName = "incident_notify_rmq.service";
        }
        logger.info("Incident Babel start with: " + ModeMetaName);
        start(ModeMetaName);
        running = true;
        worker.start();
    }

    protected void start(String ModeMetaName) {
        meta = ServiceMetaUtil.getMetaFromResourceFile(ModeMetaName);
        client = new ServiceClientImpl(meta);
        client.bindService(meta);
        client.start();
    }

    public void send(Event e) {
        if (e != null) {
            try {
                cache.put(e);
//                System.out.print("add event to send");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        try {
            worker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
    }

    private class Worker extends Thread {
        public Worker() {
            super(goods + "sender");
            this.setDaemon(true);
        }

        @Override
        public void run() {
            int idle = 0;
            while (running) {
                List<Event> events = new ArrayList<>();
                cache.drainTo(events, SendIncidentLimit);
                if (events.isEmpty()) {
                    idle++;
                    if (idle >= 3) {
                        // sleep after 3 times that no event is coming
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    idle = 0;
                    try {
                        client.notify(events, meta.getName());
                        logger.info("notified events: " + events.size());
//                        logger.debug("events: "+ events);
                        SlotMetricsHelper.getInstance().addMetrics("incident.send", (double) events.size());
                    } catch (Exception ex) {
                        logger.error("rpc:fatal:fail to send " + goods, ex);
                    }
                }
            }
            if (cache.size() > 0) {
                List<Event> events = new ArrayList<>();
                cache.drainTo(events);

                try {
                    client.notify(events, meta.getName());
                    logger.info("notified events: " + events.size());
                    SlotMetricsHelper.getInstance().addMetrics("incident.send", (double) events.size());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
