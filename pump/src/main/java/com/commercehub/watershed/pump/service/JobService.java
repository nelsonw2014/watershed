package com.commercehub.watershed.pump.service;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.commercehub.watershed.pump.model.PumpSettings;

/**
 * Created by chostetter on 11/13/15.
 */
public interface JobService {
    Job queueJob(PumpSettings pumpSettings);
    Job getJob(String jobId);
    JobPreview getJobPreview(PreviewSettings previewSettings);
}
