package com.commercehub.watershed.pump.service;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.processing.JobProcessor;
import com.commercehub.watershed.pump.processing.JobRunnable;
import com.commercehub.watershed.pump.respositories.QueryableRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.inject.Inject;

import javax.inject.Named;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class JobServiceImpl implements JobService {

    @Inject
    @Named("jobMap")
    private Map<String, Job> jobMap;

    /*
    @Inject
    private JobProcessor jobProcessor;
    */

    @Inject
    private ExecutorService executor;

    @Inject
    private QueryableRepository repository;

    @Inject
    private KinesisProducerConfiguration kinesisProducerConfiguration;

    @Inject
    private Database database;

    @Inject
    private ObjectMapper objectMapper;


    @Override
    public Job queueJob(PumpSettings pumpSettings) {
        Job job = new Job(UUID.randomUUID().toString(), pumpSettings);
        jobMap.put(job.getJobId(), job);

        executor.submit(new JobRunnable(job, kinesisProducerConfiguration, database, objectMapper));
        return job;
    }

    @Override
    public Job getJob(String jobId) {
        return jobMap.get(jobId);
    }

    @Override
    public JobPreview getJobPreview(PreviewSettings previewSettings) {
        return repository.getJobPreview(previewSettings);
    }
}
