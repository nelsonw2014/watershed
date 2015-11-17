package com.commercehub.watershed.pump.service;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobSettings;

/**
 * Created by chostetter on 11/13/15.
 */
public interface JobQueueService {
    Job queueJob(JobSettings jobSettings);
    Job getJob(String jobId);
}
