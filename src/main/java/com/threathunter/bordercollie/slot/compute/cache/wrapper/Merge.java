package com.threathunter.bordercollie.slot.compute.cache.wrapper;

/**
 * Created by daisy on 17-11-22
 */
public interface Merge {
    PrimaryData merge(PrimaryData lastMerged, String... keys);
}
