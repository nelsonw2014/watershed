package com.commercehub.watershed.pump.application;


import com.hubspot.dropwizard.guice.GuiceBundle;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PumpApplication extends Application<PumpConfiguration> {

    private static Logger log = LoggerFactory.getLogger(PumpApplication.class);
    private static final String[] defaultArgs = {"server", "./src/main/resources/pump.yaml"};

    protected GuiceBundle<PumpConfiguration> guiceBundle;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            log.warn("No arguments set, running in development mode.");
            args = defaultArgs;
        }

        new PumpApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<PumpConfiguration> bootstrap) {
        guiceBundle = buildGuiceBundleForApplication();
        bootstrap.addBundle(guiceBundle);
    }

    @Override
    public void run(PumpConfiguration configuration, Environment environment) throws Exception {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        GuiceBridge.setOverrideInjector(guiceBundle.getInjector());
        if (environment.jersey().getResourceConfig().getEndpointsInfo().contains("NONE")) {
            log.info("No resources registered, disabling jersey.");
            environment.jersey().disable();
        }
    }

    protected GuiceBundle<PumpConfiguration> buildGuiceBundleForApplication() {
        return GuiceBundle.<PumpConfiguration>newBuilder()
                .addModule(new PumpGuiceModule())
                .enableAutoConfig("com.commercehub.watershed.pump").setConfigClass(PumpConfiguration.class)
                .build();
    }
}
