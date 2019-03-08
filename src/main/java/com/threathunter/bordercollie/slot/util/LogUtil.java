package com.threathunter.bordercollie.slot.util;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Created by yy on 17-11-16.
 */
public class LogUtil {
    public static void print(VariableDataContext context, Logger logger) {
        int level = (int) context.getFromContext("level");
        Map<String, Object> variableDataMap = context.getVariableDataMap();
        logger.trace("|------context hashcode:{}, level:{}======", context.hashCode(), level);
        for (Map.Entry<String, Object> entry : variableDataMap.entrySet()) {
            logger.trace("|---key:{}\t ----value:{}", entry.getKey(), entry.getValue());
        }
        level++;
        context.addContextValue("level", level);
    }
}
