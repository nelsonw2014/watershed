package com.commercehub.watershed.pump.application;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class GuiceBridge {
    private static Injector defaultInjector = Guice.createInjector(new PumpGuiceModule());
    private static Injector overrideInjector;

    public GuiceBridge() {}

    public static Injector getInjector() {
        return overrideInjector != null? overrideInjector : defaultInjector;
    }

    public static void setOverrideInjector(Injector injector) {
        overrideInjector = injector;
    }
}
