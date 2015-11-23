package com.commercehub.watershed.pump.service;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.commercehub.watershed.pump.model.PumpSettings;

import java.util.Collection;

public interface JobService {
    Job enqueueJob(PumpSettings pumpSettings);
    Job getJob(String jobId);
    Collection<Job> getAllJobs();
    JobPreview getJobPreview(PreviewSettings previewSettings);
}
