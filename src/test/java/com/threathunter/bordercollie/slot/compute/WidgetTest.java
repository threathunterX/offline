/*
package com.threathunter.bordercollie.slot.compute;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.compute.server.ITCommonBase;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.bordercollie.slot.util.StrategyInfoCache;
import com.threathunter.model.EventMeta;
import com.threathunter.model.Property;
import com.threathunter.persistent.core.CurrentHourPersistInfoRegister;
import com.threathunter.persistent.core.EventSchema;
import com.threathunter.variable.DimensionType;
import org.assertj.core.api.Assertions;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

*/
/**
 * Created by yy on 17-11-23.
 *//*

public class WidgetTest {
    @Ignore
    @Test
    public void time() {
        long l = System.currentTimeMillis();
        System.out.println(l);
        System.out.println(new Date(l));
        String str = String.valueOf(l);
        System.out.println("timestamp = " + str);
        //1511589600.0
        String substring = str.substring(0, 10);
        System.out.println(String.format("sub timestamp=%s", substring));
        String timestampForStartOfDay = SlotUtils.getTimestampForStartOfDay(substring);
        System.out.println(String.format("start of day = %s", timestampForStartOfDay));
    }

    @Ignore
    @Test
    public void test1() {
        Double timestamp = (Double) 1511589600.0;

        Timestamp t = new Timestamp(timestamp.longValue() * 1000);
        String queryHour = new DateTime(t.getTime()).toString(SlotUtils.slotTimestampFormat).toString();
        System.out.println(queryHour);
    }

    @Ignore
    @Test
    public void test2() {
        DimensionType global = DimensionType.getDimension("global");
        Assertions.assertThat(global).isNotNull();
        System.out.println(global.name());
    }

    @Ignore
    @Test
    public void test3() {
        DimensionType global = DimensionType.getDimension("GLOBAL");
        Assertions.assertThat(global).isNotNull();
        System.out.println(global.name());
    }

    @Ignore
    @Test
    public void test4() {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> eventsObjects;
        try {
//            File file = new File("/home/yy/source/bordercollie/profiles/dev/events_schema.json");
            File file = new File("/home/yy/source/bordercollie/src/ITLauncher/resources/events_schema.json");
        */
/*    URL in=ITCommonBase.class.getClassLoader().getResource("events_schema1.json");
          List   eventsObjects = mapper.reader(List.class).readValue(in);*//*

            CurrentHourPersistInfoRegister.getInstance().update(file);
            docheck();
        } catch (Exception e) {
            throw new RuntimeException("strategy cache init", e);
        }
    }

    private void docheck() {
        int i = 0;
        for (EventMeta meta : CurrentHourPersistInfoRegister.getInstance().getEventMetas()) {
            System.out.println("size=" + CurrentHourPersistInfoRegister.getInstance().getEventMetas().size());
            try {
                EventSchema es = CurrentHourPersistInfoRegister.getInstance().getEventLogSchema(meta.getName());
                for (Property m : es.getProperties()) {
//                    Map<String, String> map = (Map<String, String>) m;
//                    Property p=Property ;
                    String field = m.getName();
//                    m.getType()
                }
            } catch (Exception e) {
                System.out.println("" + meta.getName() + (++i));// e.printStackTrace();
            }
        }
    }

    private void initStrategyInfoCache() {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> strategyObjects;
        try {
            URL in = ITCommonBase.class.getClassLoader().getResource("events_chema1.json");
            strategyObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            throw new RuntimeException("strategy cache init", e);
        }
        StrategyInfoCache.getInstance().update(strategyObjects);
    }

    @Ignore
    @Test
    public void test() {
        System.out.println(Double.valueOf("1141.0").longValue() * 1000);
    }

    @Ignore
    @Test
    public void testsnipper() {
        List<Integer> numList = Arrays.asList(10, 21, 31, 40, 59, 60);
        numList.forEach(x -> {
            if (x % 2 == 0) {
                return;
            }
            System.out.println(x);
        });
    }


    @Test
    public void testArray() {
        byte[] bytes = new byte[]{1, 1, 1, 2, 2};
        System.arraycopy(bytes, 2, bytes, 3, 2);

        System.out.println(bytes);
    }

    @Ignore
    @Test
    public void testRightShift() {
        int x = -1;
        System.out.println(x >> 1);
    }

    @Test
    public void testFF() {
        byte a = (byte) 0xff;
        int b = 0xff;
        System.out.printf("%d\n", a);
        System.out.printf("%x\n", a);
        System.out.println(b);
    }

    @Test
    public void testCheck(){
        System.out.println("====================yyyyyy========================");
        System.out.println("====================yyyyyy========================");
    }
}
*/
