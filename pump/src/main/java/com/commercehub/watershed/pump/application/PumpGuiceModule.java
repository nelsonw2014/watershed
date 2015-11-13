package com.commercehub.watershed.pump.application;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.InputStream;
import java.util.Properties;

public class PumpGuiceModule extends AbstractModule {

    Properties defaultProperties;
    private static final String DEFAULT_PROPERTIES_FILE = "pump.properties";
    private static final Logger log = LoggerFactory.getLogger(PumpGuiceModule.class);

    @Override
    protected void configure() {
        defaultProperties = getDefaultPropertiesFromFile();
    }

    @Provides
    @Singleton
    @Named("applicationProperties")
    private Properties getProperties(){
        return defaultProperties;
    }

    private Properties getDefaultPropertiesFromFile() {
        Properties properties = new Properties();
        String propertyFileName = DEFAULT_PROPERTIES_FILE;
        try {
            InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFileName);
            properties.load(configStream);
            Names.bindProperties(binder(), properties);
        } catch (Exception ex) {
            log.error("Had a problem loading in application properties: " + propertyFileName, ex);
        }

        return properties;
    }
}
