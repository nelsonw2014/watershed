package com.commercehub.watershed.pump.service;

import com.commercehub.watershed.pump.application.factories.JobFactory;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.processing.JobRunnable;
import com.commercehub.watershed.pump.respositories.QueryableRepository;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class JobServiceImpl implements JobService {

    @Inject
    private Map<String, Job> jobMap;

    @Inject
    private Provider<JobRunnable> jobRunnableProvider;

    @Inject
    private ExecutorService executor;

    @Inject
    private QueryableRepository repository;

    @Inject
    private JobFactory jobFactory;

    @Override
    public Job enqueueJob(PumpSettings pumpSettings) {
        Job job = jobFactory.create(UUID.randomUUID().toString(), pumpSettings);
        jobMap.put(job.getJobId(), job);

        executor.submit(jobRunnableProvider.get().with(job));
        return job;
    }

    @Override
    public Job getJob(String jobId) {
        return jobMap.get(jobId);
    }

    @Override
    public Collection<Job> getAllJobs(){
        return jobMap.values();
    }

    @Override
    public JobPreview getJobPreview(PreviewSettings previewSettings) throws SQLException {
        return repository.getJobPreview(previewSettings);
    }
}
