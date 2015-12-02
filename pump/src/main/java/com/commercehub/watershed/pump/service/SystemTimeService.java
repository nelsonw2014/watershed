package com.commercehub.watershed.pump.service;

public class SystemTimeService implements TimeService{
    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
