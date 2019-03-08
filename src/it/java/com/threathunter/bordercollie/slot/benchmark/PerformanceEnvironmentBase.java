package com.threathunter.bordercollie.slot.benchmark;

import com.threathunter.bordercollie.slot.compute.SlotEngine;
import com.threathunter.mock.simulator.HttpDynamicEventsAction;
import com.threathunter.model.Event;
import com.threathunter.persistent.core.api.SequenceReadContext;
import com.threathunter.persistent.core.api.Visitor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 2.16
 */

@Slf4j
public abstract class PerformanceEnvironmentBase {
    public static SlotEngine engine;
   /* @Rule
    public TestRule watcher = new TestWatcher() {
        private long start;

        protected void starting(Description description) {
            log.info("===================================");
            log.info("Starting test: " + description.getMethodName());
            start = System.currentTimeMillis();
        }

        @Override
        protected void finished(Description description) {
            long end = System.currentTimeMillis();
            log.info("Test " + description.getMethodName() + " took " + (end - start) + "ms");
            log.info("===================================");
        }
    };*/



/*    @BeforeClass
    public static void setUp(){

    }*/
    /**
     * 250M 2017122201/log  234069 events --> 1 event 约 1.06 K 真实数据 properties 比较多
     * 532M 2017122322/log   name:testSlotEngine_500MEvents	size:499853	cost:322899
     */


    public static int count = 0;

    /*public static void increaseCounter() {
        count++;
    }*/

    public Event[] generateRandom5KEvents() {
        List<Event> list = new ArrayList<>();

        for (int i = 0; i < 5000; i++) {
            //5k
            HttpDynamicEventsAction action = new HttpDynamicEventsAction("localhost", 1);
            action.constructEvents();
            list.addAll(action.getEvents());
        }
        decorateEvent(list);
        return list.toArray(new Event[]{});
    }

    public Event[] generateRandom5WEvents() {
        List<Event> list = new ArrayList<>();

        for (int i = 0; i < 50000; i++) {
            //5w
            HttpDynamicEventsAction action = new HttpDynamicEventsAction("localhost", 1);
            action.constructEvents();
            list.addAll(action.getEvents());
        }
        decorateEvent(list);
        return list.toArray(new Event[]{});
    }

    public Event[] generateRandom10WEvents() {
        List<Event> list = new ArrayList<>();

        for (int i = 0; i < 100000; i++) {
            //10W
            HttpDynamicEventsAction action = new HttpDynamicEventsAction("localhost", 1);
            action.constructEvents();
            list.addAll(action.getEvents());
        }
        decorateEvent(list);
        return list.toArray(new Event[]{});
    }

    protected void compositeCalculate(String[] dirs, String name) {
        log.info("================{}:start======================", name);
        long start = System.currentTimeMillis();
        List<Event> list = new ArrayList<>();
        final AtomicInteger count = new AtomicInteger();
        Visitor visitor = new Visitor() {
            @Override
            public void visit(Event event) {
                decorateEvent(event);
                engine.add(event);
                count.incrementAndGet();
            }
        };

        engine.start();
        for (String dir : dirs) {
            SequenceReadContext context = new SequenceReadContext(dir, visitor);
            context.startQuery();
            engine.save();
            context.endQuery();
        }
        engine.stop();
        long end = System.currentTimeMillis();
        log.info("name:{}\tsize:{}\tcost:{}", name, count.get(), end - start);
        log.info("================{}:end======================", name);

    }

    public void calculate(String dir, String name) {
        log.info("================{}:start======================", name);
        long start = System.currentTimeMillis();
        List<Event> list = new ArrayList<>();
        final AtomicInteger count = new AtomicInteger();
        Visitor visitor = new Visitor() {
            @Override
            public void visit(Event event) {
                decorateEvent(event);
                engine.add(event);
                count.incrementAndGet();
            }
        };

        engine.start();
        SequenceReadContext context = new SequenceReadContext(dir, visitor);
        context.startQuery();
        engine.save();
        context.endQuery();
        engine.stop();
        long end = System.currentTimeMillis();
        log.info("name:{}\tsize:{}\tcost:{}", name, count.get(), end - start);
        log.info("================{}:end======================", name);
    }


    public void decorateEvent(List<Event> events) {
        for (Event event : events)
            decorateEvent(event);
    }

    public abstract void decorateEvent(Event event);

}
