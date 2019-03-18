package com.threathunter.bordercollie.slot.api;

/**
 * 
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
