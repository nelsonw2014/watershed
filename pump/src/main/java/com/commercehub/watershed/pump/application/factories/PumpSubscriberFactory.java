package com.commercehub.watershed.pump.application.factories;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.processing.Pump;
import com.commercehub.watershed.pump.processing.PumpSubscriber;

public interface PumpSubscriberFactory {
    PumpSubscriber create(Job job, Pump pump);
}
