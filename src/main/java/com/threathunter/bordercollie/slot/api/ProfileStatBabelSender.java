package com.threathunter.bordercollie.slot.api;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.impl.ServiceClientImpl;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.model.Event;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by toyld on 2/28/17.
 */
public class ProfileStatBabelSender extends BabelSender {
    private static final Logger logger = LoggerFactory.getLogger(ProfileStatBabelSender.class);
    protected final BlockingQueue<Event> cache;
    private final Worker worker;
    protected ServiceClientImpl client;
    protected ServiceMeta meta;
    protected String goods;
    private volatile boolean running = false;

    public ProfileStatBabelSender() {
        goods = "profile stat";
        worker = new Worker();
        this.cache = new LinkedBlockingQueue<>();
//        ShutdownHookManager.get().addShutdownHook(() -> stop(), 2);
    }

    public static ProfileStatBabelSender getInstance() {
        logger.info("come to get instance for profile send");
        return (ProfileStatBabelSender) getInstance("com.threathunter.bordercollie.slot.api.ProfileStatBabelSender");
    }

    public void start(boolean redisMode) {
        String ModeMetaName = null;
        if (redisMode) {
            ModeMetaName = "profile_stat_notify_redis.service";
        } else {
            ModeMetaName = "profile_stat_notify_rmq.service";
        }
        logger.info("Profile Babel start with: " + ModeMetaName);
        start(ModeMetaName);
        running = true;
        worker.start();
    }

    protected void start(final String ModeMetaName) {
        meta = ServiceMetaUtil.getMetaFromResourceFile(ModeMetaName);
        client = new ServiceClientImpl(meta);
        client.bindService(meta);
        client.start();
    }

    public void send(Event e) {
        if (e != null) {
            try {
                cache.put(e);
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
        Gson gson = new Gson();

        public Worker() {
            super(goods + "sender");
            this.setDaemon(true);
        }

        @Override
        public void run() {
            int idle = 0;
            while (running) {
                List<Event> events = new ArrayList<>();
                cache.drainTo(events, 1); // make sure send 100 key per time aka 1 event.
                if (events.isEmpty()) {
                    idle++;
                    if (idle >= 3) {
                        // sleep after 3 times that no event is coming
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    idle = 0;
                    try {
                        client.notify(events.get(0), meta.getName());
                        logger.info(">>>send profile event json content: {}", (gson.toJson(events.get(0))));
                        SlotMetricsHelper.getInstance().addMetrics("profilestat.send", (double) 1);
                    } catch (Exception ex) {
                        logger.error("rpc:fatal:fail to send " + goods, ex);
                    }
                }
            }
            if (cache.size() > 0) {
                logger.warn("cache still have :" + cache.size());
                try {
                    Event event = cache.take();
                    client.notify(event, meta.getName());
                    logger.info((gson.toJson(event)));
                    SlotMetricsHelper.getInstance().addMetrics("profilestat.send", (double) 1);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("rpc:fatal:fail to send " + goods, e);
                }
            }
        }


    }

}
