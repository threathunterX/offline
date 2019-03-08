package com.threathunter.bordercollie.slot.compute.cache.builder.util;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 1.5
 */
public class CacheCommonUtilTest {

    @Test
    public void testIsEmpty_withEmptyString() {
        boolean result = CacheCommonUtil.isEmpty("");
        assertThat(result).isTrue();
    }

    @Test
    public void testIsEmpty_withNull() {
        boolean result = CacheCommonUtil.isEmpty(null);
        assertThat(result).isTrue();
    }

    @Test
    public void testIsEmpty_withNotNull() {
        boolean result = CacheCommonUtil.isEmpty("abcdefg");
        assertThat(result).isFalse();
    }
}
