package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.config.CommonDynamicConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 
 */
public class ITCommonBaseTest extends ITCommonBase {
    @Test
    public void testConfig() {
        assertThat(CommonDynamicConfig.getInstance().getString("babel_server")).isEqualTo("redis");

    }
}
