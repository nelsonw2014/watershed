package com.commercehub.watershed.pump.service;

import com.commercehub.watershed.pump.model.Job;
import com.google.inject.Inject;

import javax.inject.Named;
import java.util.Map;
import java.util.Queue;

/**
 * Created by chostetter on 11/13/15.
 */
public class JobProcessorImpl implements JobProcessor {
    @Inject
    @Named("jobQueue")
    private Queue<Job> jobQueue;

    @Inject
    @Named("jobMap")
    private Map<String, Job> jobMap;

    private Boolean isProcessing;

    private Job getNextJobOnQueue(){
        return jobQueue.remove();
    }

    public void startProcessing() {

        if(isProcessing) { return; }

        isProcessing = true;

        while(jobQueue.size() > 0) {
            processJob(getNextJobOnQueue());
        }

        isProcessing = false;
    }

    private void processJob(Job job){

    }
}
