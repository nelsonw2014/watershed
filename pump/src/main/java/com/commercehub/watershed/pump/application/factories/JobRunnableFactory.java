package com.commercehub.watershed.pump.application.factories;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.processing.JobRunnable;

public interface JobRunnableFactory {
    JobRunnable create(Job job);
}
