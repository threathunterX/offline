package com.threathunter.bordercollie.slot.compute.cache.wrapper;

/**
 * 
 */
public interface Merge {
    PrimaryData merge(PrimaryData lastMerged, String... keys);
}
