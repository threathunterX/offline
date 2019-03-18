package com.threathunter.mock.util;

import com.threathunter.model.Event;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Random;

/**
 * 
 */
public class OrderSubmitEventMaker implements EventMaker {
    private HttpDynamicEventMaker httpDynamicEventMaker;
    int mockCount = 10;

    private Random r = new Random();
    private String[] commonRandomUserName;
    private String[] commonRandomMerchant;
    private String[] commonRandomMobile;
    private String[] commonRandomCountry;
    private String[] commonRandomProvince;
    private String[] commonRandomCity;
    private String[] commonRandomRealname;

    public OrderSubmitEventMaker(int mockCount) {
        this.mockCount = mockCount;
        httpDynamicEventMaker = new HttpDynamicEventMaker(mockCount);
        initial();
    }

    @Override
    public Event nextEvent() {
        Event event = httpDynamicEventMaker.nextEvent();
        event.setName("ORDER_SUBMIT");

        int index = r.nextInt(commonRandomUserName.length);
        event.getPropertyValues().put("user_name", commonRandomUserName[index]);
        event.getPropertyValues().put("merchant", commonRandomMerchant[index]);
        event.getPropertyValues().put("merchant_name", commonRandomMerchant[index]);
        event.getPropertyValues().put("product_id", RandomStringUtils.randomAlphanumeric(8));
        event.getPropertyValues().put("order_id", RandomStringUtils.randomAlphanumeric(8));
        event.getPropertyValues().put("transaction_id", RandomStringUtils.randomAlphanumeric(8));
        event.getPropertyValues().put("order_money_amount", r.nextLong() * 1000);
        event.getPropertyValues().put("order_coupon_amount", r.nextLong() * 1000);
        event.getPropertyValues().put("order_point_amount", r.nextLong() * 1000);
        event.getPropertyValues().put("receive_mobile", commonRandomMobile[index]);
        event.getPropertyValues().put("receiver_address_country", commonRandomCountry[index]);
        event.getPropertyValues().put("receiver_address_province", commonRandomProvince[index]);
        event.getPropertyValues().put("receiver_address_city", commonRandomCity[index]);
        event.getPropertyValues().put("receiver_realname", commonRandomRealname[index]);
        event.getPropertyValues().put("receiver_address_detail", RandomStringUtils.randomAlphanumeric(10));
        event.getPropertyValues().put("result", "T");
        event.getPropertyValues().put("product_type", "type1");
        event.getPropertyValues().put("product_attribute", "attr1");
        event.getPropertyValues().put("product_location", "location1");
        event.getPropertyValues().put("product_count", r.nextInt(10));
        event.getPropertyValues().put("product_total_count", r.nextInt(10));
        return event;
    }

    @Override
    public void close() {

    }

    public String getEventName() {
        return "ORDER_SUBMIT";
    }

    private void initial() {
        this.commonRandomCity = new String[mockCount];
        this.commonRandomCountry = new String[mockCount];
        this.commonRandomMerchant = new String[mockCount];
        this.commonRandomMobile = new String[mockCount];
        this.commonRandomProvince = new String[mockCount];
        this.commonRandomRealname = new String[mockCount];
        this.commonRandomUserName = new String[mockCount];

        for (int i = 0; i < mockCount; i++) {
            this.commonRandomCity[i] = "city" + i;
            this.commonRandomCountry[i] = "country" + i;
            this.commonRandomMerchant[i] = "merchant" + i;
            this.commonRandomProvince[i] = "province" + i;
            this.commonRandomUserName[i] = "user_" + i;
            this.commonRandomRealname[i] = "jack_" + i;
            this.commonRandomMobile[i] = "1812344550" + i;
        }
    }
}
