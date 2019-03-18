package com.threathunter.bordercollie.slot.util;

/**
 * 
 */
public class PathHelper {
    public static String getModulePath() {
        return System.getProperties().get("user.dir").toString();
    }
}
