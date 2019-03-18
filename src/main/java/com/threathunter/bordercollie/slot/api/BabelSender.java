package com.threathunter.bordercollie.slot.api; /**
 * 
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class BabelSender {
    /**
     * A com.threathunter.nebula.slot.offline.BabelSender Singleton factory. This factory will store the instance by name to keep the Singleton.
     */
    private static final Logger logger = LoggerFactory.getLogger(BabelSender.class);
    private static HashMap<String, Object> singletonMap = new HashMap<String, Object>();

    static {
        BabelSender x = new BabelSender();
        singletonMap.put(x.getClass().getName(), x);
    }


    protected BabelSender() {
    }

    public static BabelSender getInstance(String name) {
        if (name == null) {
            name = "com.threathunter.bordercollie.slot.api.BabelSender";
        }

        if (singletonMap.get(name) == null) {
            try {
                singletonMap.put(name, Class.forName(name).newInstance());
            } catch (ClassNotFoundException cnf) {
                System.out.println("Couldn't find class " + name);
            } catch (InstantiationException ie) {
                System.out.println("Couldn't instantiate an object of type " + name);
            } catch (IllegalAccessException ia) {
                System.out.println("Couldn't access class " + name);
            }
        }
        return (BabelSender) (singletonMap.get(name));
    }


}
