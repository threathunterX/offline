package com.threathunter.mock.util;

import com.threathunter.bordercollie.slot.util.JsonFileReader;
import com.threathunter.bordercollie.slot.util.StrategyInfoCache;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import com.threathunter.model.Event;
import com.threathunter.persistent.core.CurrentHourPersistInfoRegister;
import com.threathunter.persistent.core.io.EventOfflineWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by daisy on 17-11-23
 */
@Ignore
public class PersistentDataUtils {
    private static EventOfflineWriter writer = EventOfflineWriter.getInstance();

    @BeforeClass
    public static void setup() throws IOException {
        String path = "/home/daisy/workplace/Code_threathunter/current_bordercollie/bordercollie/src/test/resources/events.json";
//        String path = String.format("events_schema.json", System.getProperty("user.dir"));
        CurrentHourPersistInfoRegister.getInstance().update(path);
        writer.start();
    }

    @AfterClass
    public static void teardown() {
        writer.stop();
    }

    @Test
    public void genPersistentData() {
        OrderSubmitEventMaker omaker = new OrderSubmitEventMaker(10);
        TransactionEscrow tmaker = new TransactionEscrow(10);
        CommonDynamicConfig.getInstance().addOverrideProperty("metrics_server", "redis");
        MetricsAgent.getInstance().start();
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                Event event = omaker.nextEvent();
                Event event2 = tmaker.nextEvent();

                event.getPropertyValues().put("c_ip", "127.0.0.1");
                event.getPropertyValues().put("uid", "uid_000001");
                event.getPropertyValues().put("product_location", "location1");
                event.getPropertyValues().put("product_count", 1);
                event.getPropertyValues().put("merchant_name", "merchant1");
                event.getPropertyValues().put("platform","h5");

                event2.getPropertyValues().put("c_ip", "127.0.0.1");
                event2.getPropertyValues().put("uid", "uid_000001");
                event2.getPropertyValues().put("escrow_type", "taobao");
                event2.getPropertyValues().put("platform","h5");
                event2.getPropertyValues().put("order_money_amount", 500);
                writer.addLog(event);
                writer.addLog(event2);
            } else {
                Event event = omaker.nextEvent();
                Event event2 = tmaker.nextEvent();

                event.getPropertyValues().put("c_ip", "127.0.0.2");
                event.getPropertyValues().put("uid", "uid_000002");
                event.getPropertyValues().put("product_count", 2);
                event.getPropertyValues().put("product_location", "location2");
                event.getPropertyValues().put("merchant_name", "merchant2");
                event.getPropertyValues().put("platform","h5");

                event2.getPropertyValues().put("c_ip", "127.0.0.2");
                event2.getPropertyValues().put("uid", "uid_000002");
                event2.getPropertyValues().put("escrow_type","tengxun");
                event2.getPropertyValues().put("platform","h5");
                event2.getPropertyValues().put("order_money_amount", 1000);
                writer.addLog(event);
                writer.addLog(event2);
            }
        }
    }

    @Test
    public void genIncidentData() {
        IncidentEventMaker maker = new IncidentEventMaker(10);

        String ip1 = "127.0.0.1";
        String ip2 = "127.0.0.2";
        String did1 = "did1";
        String did2 = "did2";
        for (int i = 0; i < 10; i++) {
            Event event = maker.nextEvent();
            if (i % 2 == 0) {
                event.getPropertyValues().put("c_ip", ip1);
                event.getPropertyValues().put("geo_city", "上海市");
                if (i % 3 == 0) {
                    event.getPropertyValues().put("did", did1);
                } else {
                    event.getPropertyValues().put("did", did2);
                }
            } else {
                event.getPropertyValues().put("c_ip", ip2);
                event.getPropertyValues().put("geo_city", "浙江省");
                if (i % 3 == 0) {
                    event.getPropertyValues().put("did", did1);
                } else {
                    event.getPropertyValues().put("did", did2);
                }
            }
            writer.addLog(event);
        }
    }
}
