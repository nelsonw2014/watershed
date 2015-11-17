package com.commercehub.watershed.pump.application;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.commercehub.watershed.pump.IsolatedConnectionProvider;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.respositories.DrillRepository;
import com.commercehub.watershed.pump.respositories.QueryableRepository;
import com.commercehub.watershed.pump.service.JobService;
import com.commercehub.watershed.pump.service.JobServiceImpl;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.github.davidmoten.rx.jdbc.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        bind(JobService.class).to(JobServiceImpl.class);
        //bind(JobProcessor.class).to(JobProcessorImpl.class);
        bind(QueryableRepository.class).to(DrillRepository.class);
    }

    @Provides
    @Singleton
    @Named("applicationProperties")
    private Properties getProperties(){
        return defaultProperties;
    }

    @Provides
    @Singleton
    private Queue<Job> getJobQueue(){
        return jobQueue;
    }

    @Provides
    @Singleton
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

    /*
    @Provides
    private JobRunnable jobRunnableProvider(Queue<Job> jobQueue, KinesisProducerConfiguration kinesisProducerConfiguration, Database database){
        return new JobRunnable(jobQueue, kinesisProducerConfiguration, database, objectMapper);
    }
    */

    @Provides
    @Singleton
    private KinesisProducerConfiguration configureKinesis() {
        KinesisProducerConfiguration kinesisConfig = new KinesisProducerConfiguration();
        kinesisConfig.setAggregationEnabled(false); //TODO enable KPL aggregation after Filter's KCL is upgraded; it should be a big help.
        kinesisConfig.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        kinesisConfig.setRegion(Regions.US_EAST_1.getName());
        kinesisConfig.setRecordTtl(Integer.MAX_VALUE);  //Maybe not the best idea to use MAX_VALUE
        // Pump works more smoothly when shards are not saturated, so 95% is a good maximum rate.
        // May be lowered further to share capacity with running applications.
        kinesisConfig.setRateLimit(50);

        //TODO set more Kinesis Configuration options as appropriate
        return kinesisConfig;
    }

    @Provides
    @Singleton
    private Database connectDatabase() {
        String jdbcUrl = "jdbc:drill:drillbit=localhost:31010";
        String jdbcUsername = "admin";
        String jdbcPassword = "admin";
        String driverClass = "org.apache.drill.jdbc.Driver";
        Database database;
        try {
            log.info("Testing database connection.");
            ConnectionProvider cp = IsolatedConnectionProvider.get(jdbcUrl, jdbcUsername, jdbcPassword, driverClass);
            cp.get().close();
            database = Database.from(cp);
            log.info("Database connectivity verified.");
        } catch (Exception e) {
            log.error("Could not establish a preliminary database connection", e);
            System.exit(2);
            database = null; //thanks javac.
        }
        return database;
    }

    @Provides
    @Singleton
    protected ExecutorService getExecutor(@Named("numConcurrentJobs") int numConcurrentJobs) {
        return Executors.newFixedThreadPool(numConcurrentJobs);
    }
}
