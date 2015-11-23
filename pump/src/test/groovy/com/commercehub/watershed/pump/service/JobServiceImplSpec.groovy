package com.commercehub.watershed.pump.service

import com.commercehub.watershed.pump.model.Job
import com.commercehub.watershed.pump.model.JobPreview
import com.commercehub.watershed.pump.model.PreviewSettings
import com.commercehub.watershed.pump.model.PumpSettings
import com.commercehub.watershed.pump.processing.JobRunnable
import com.commercehub.watershed.pump.respositories.QueryableRepository
import com.google.inject.Provider
import spock.lang.Specification

import java.util.concurrent.ExecutorService

class JobServiceImplSpec extends Specification {

    JobService jobService
    Map<String, Job> jobMap
    Provider<JobRunnable> jobRunnableProvider
    ExecutorService executor
    QueryableRepository repository
    JobRunnable jobRunnable

    PumpSettings pumpSettings = new PumpSettings(queryIn: "select * from foo", streamOut: "MyStream")
    PreviewSettings previewSettings = new PreviewSettings(queryIn: "select * from foo", previewCount: 3)

    def setup() {
        jobMap = new HashMap<>()
        jobRunnableProvider = Mock(Provider)
        executor = Mock(ExecutorService)
        repository = Mock(QueryableRepository)
        jobRunnable = Mock(JobRunnable)

        jobService = new JobServiceImpl(
                jobMap: jobMap,
                jobRunnableProvider: jobRunnableProvider,
                executor: executor,
                repository: repository)
    }

    def "enqueueJob queues a job with the executor and adds it to the map"(){
        when:
        Job job = jobService.enqueueJob(pumpSettings)

        then:
        1 * jobRunnableProvider.get() >> jobRunnable
        1 * jobRunnable.withJob(_ as Job) >> jobRunnable
        1 * executor.submit(_ as JobRunnable)
        jobMap.size() == 1
        jobMap.get(job.jobId) == job
        job.pumpSettings == pumpSettings
    }

    def "getJob returns job from jobMap"(){
        setup:
        String jobId = UUID.randomUUID().toString()
        jobMap.put(jobId, new Job(jobId, pumpSettings))

        when:
        Job job = jobService.getJob(jobId)

        then:
        job == jobMap.get(jobId)
    }

    def "getJob returns null if not found in jobMap"(){
        setup:
        String jobId = UUID.randomUUID().toString()

        when:
        Job job = jobService.getJob(jobId)

        then:
        job == null
    }

    def "getAllJobs returns jobMap values"(){
        setup:
        String jobId = UUID.randomUUID().toString()
        jobMap.put(jobId, new Job(jobId, pumpSettings))

        when:
        Collection<Job> jobs = jobService.getAllJobs()

        then:
        jobs == jobMap.values()
    }

    def "getJobPreview returns jobPreview from repository"(){
        setup:
        JobPreview dummyJobPreview = new JobPreview(200, [["field1": "value1"], ["field2": "value2"], ["field2": "value2"]])

        when:
        JobPreview jobPreview = jobService.getJobPreview(previewSettings)

        then:
        1 * repository.getJobPreview(previewSettings) >> dummyJobPreview
        jobPreview == dummyJobPreview
    }
}
