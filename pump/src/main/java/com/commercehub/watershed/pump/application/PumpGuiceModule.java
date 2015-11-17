package com.commercehub.watershed.pump.application;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.service.JobProcessor;
import com.commercehub.watershed.pump.service.JobProcessorImpl;
import com.commercehub.watershed.pump.service.JobQueueService;
import com.commercehub.watershed.pump.service.JobQueueServiceImpl;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.InputStream;
import java.util.*;

public class PumpGuiceModule extends AbstractModule {
    private static final String DEFAULT_PROPERTIES_FILE = "pump.properties";
    private static final Logger log = LoggerFactory.getLogger(PumpGuiceModule.class);

    private Properties defaultProperties;
    private ObjectMapper objectMapper;

    private Queue<Job> jobQueue = new LinkedList<>();
    private Map<String, Job> jobMap = new HashMap<>();

    @Override
    protected void configure() {
        defaultProperties = getDefaultPropertiesFromFile();
        objectMapper = configureObjectMapper();
        bind(JobQueueService.class).to(JobQueueServiceImpl.class);
        bind(JobProcessor.class).to(JobProcessorImpl.class);
    }

    @Provides
    @Singleton
    @Named("applicationProperties")
    private Properties getProperties(){
        return defaultProperties;
    }

    @Provides
    @Singleton
    @Named("jobQueue")
    private Queue<Job> getJobQueue(){
        return jobQueue;
    }

    @Provides
    @Singleton
    @Named("jobMap")
    private Map<String, Job> getJobMap(){
        return jobMap;
    }

    @Provides
    @Singleton
    private ObjectMapper getObjectMapper(){
        return objectMapper;
    }

    private ObjectMapper configureObjectMapper(){
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JodaModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        objectMapper.configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true);

        return objectMapper;
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
