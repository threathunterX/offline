package com.threathunter.bordercollie.slot.util;

/**
 * Created by daisy on 16/6/28.
 */
public class PathHelper {
    public static String getModulePath() {
        return System.getProperties().get("user.dir").toString();
    }
}
