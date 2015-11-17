package com.commercehub.watershed.pump.service;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobSettings;
import com.commercehub.watershed.pump.model.QueryAndEmitJob;
import com.commercehub.watershed.pump.model.QueryJob;
import com.google.inject.Inject;

import javax.inject.Named;
import java.security.InvalidParameterException;
import java.util.*;

public class JobQueueServiceImpl implements JobQueueService {

    @Inject
    @Named("jobQueue")
    private Queue<Job> jobQueue;

    @Inject
    @Named("jobMap")
    private Map<String, Job> jobMap;

    private Boolean isProcessing;

    @Inject
    private JobProcessor jobProcessor;

    @Override
    public Job queueJob(JobSettings jobSettings) {
        Job job;
        switch(jobSettings.getJobType()){
            case QUERY_AND_EMIT:
                job = new QueryAndEmitJob(UUID.randomUUID().toString(), jobSettings);
                break;

            case QUERY_ONLY:
                job = new QueryJob(UUID.randomUUID().toString(), jobSettings);
                break;

            default:
                throw new InvalidParameterException(jobSettings.getJobType().toString() + " is not a valid job type.");
        }
        jobMap.put(job.getJobId(), job);
        jobQueue.add(job);

        jobProcessor.startProcessing();
        return job;
    }

    @Override
    public Job getJob(String jobId) {
        return jobMap.get(jobId);
    }
}
