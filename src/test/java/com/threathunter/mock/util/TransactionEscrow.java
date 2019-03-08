package com.threathunter.mock.util;

import com.threathunter.model.Event;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Random;

/**
 * Created by daisy on 18-1-17
 */
public class TransactionEscrow implements EventMaker {
    private HttpDynamicEventMaker httpDynamicEventMaker;
    int mockCount = 10;

    private Random r = new Random();
    private String[] commonRandomCardNumber;

    public TransactionEscrow(int mockCount) {
        this.mockCount = mockCount;
        httpDynamicEventMaker = new HttpDynamicEventMaker(mockCount);
        initial();
    }

    @Override
    public String getEventName() {
        return "TRANSACTION_ESCROW";
    }

    @Override
    public Event nextEvent() {
        Event event = httpDynamicEventMaker.nextEvent();
        int index = r.nextInt(commonRandomCardNumber.length);
        event.getPropertyValues().put("transaction_id", RandomStringUtils.randomNumeric(11));
        long amount = r.nextInt(100000);
        event.getPropertyValues().put("pay_amount", amount);
        event.getPropertyValues().put("escrow_type", "type1");
        event.getPropertyValues().put("escrow_account", commonRandomCardNumber[index]);
        event.getPropertyValues().put("result", "T");
        event.getPropertyValues().put("order_money_amount", 100);

        event.setName("TRANSACTION_ESCROW");
        return event;
    }

    @Override
    public void close() {

    }

    private void initial() {
        this.commonRandomCardNumber = new String[mockCount];

        for (int i = 0; i < mockCount; i++) {
            this.commonRandomCardNumber[i] = "card_number_" + i;
        }
    }
}
