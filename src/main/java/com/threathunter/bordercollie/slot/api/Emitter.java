package com.threathunter.bordercollie.slot.api;

/**
 * Created by yy on 17-11-21.
 */
public interface Emitter {
    void render();
    boolean start();
    boolean stop();
    boolean startDimension();
    boolean stopDimension();
    boolean startKey();
    boolean stopKey();
    boolean startVariable();
    boolean stopVariable();
}
