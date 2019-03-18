package com.threathunter.bordercollie.slot.compute;

import com.threathunter.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 
 */
public class SlotMergeQuery {
    private final SlotComputable engine;
    private static Logger logger = LoggerFactory.getLogger(SlotMergeQuery.class);
    private Map<Identifier, Object> mergeKey;
    private List<Identifier> mergeVariable;

    public SlotMergeQuery(SlotComputable engine) {
        this.engine = engine;
    }

    public void setMergeVariable(List<Identifier> mergeVariable) {
        this.mergeVariable = mergeVariable;
    }

    public void setMergerKey(Map<Identifier, Object> mergeKey) {
        this.mergeKey = mergeKey;
    }
}
