package com.commercehub.watershed.pump.application.factories;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.PumpSettings;

public interface JobFactory {
    Job create(String jobId, PumpSettings pumpSettings);
}
