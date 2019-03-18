package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;

import java.util.List;

/**
 * 
 */
public abstract class NodePrimaryData {
    private PrimaryData[] wrapperPrimaryData;

    public PrimaryData[] getWrapperPrimaryData() {
        return wrapperPrimaryData;
    }

    public void setWrapperPrimaryData(PrimaryData[] wrapperPrimaryData) {
        this.wrapperPrimaryData = wrapperPrimaryData;
    }

    public abstract Object getResult();
}
