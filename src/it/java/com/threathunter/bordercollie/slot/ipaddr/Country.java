package com.threathunter.bordercollie.slot.ipaddr;

/**
 * Represents the country codes. Used to refer to specific configuration files.
 *
 * @author Nirmalya Ghosh
 */
public enum Country {
    AUSTRALIA("AU"),
    CHINA("CN"),
    INDIA("IN"),
    SINGAPORE("SG");

    private String code;

    private Country(String countryCode) {
        this.code = countryCode;
    }

    public String getCode() {
        return code;
    }
}