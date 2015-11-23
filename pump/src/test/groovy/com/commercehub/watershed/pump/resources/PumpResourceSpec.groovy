package com.commercehub.watershed.pump.resources

import com.commercehub.watershed.pump.application.GuiceBridge
import com.commercehub.watershed.pump.application.PumpGuiceModule
import com.commercehub.watershed.pump.model.Job
import com.commercehub.watershed.pump.model.JobPreview
import com.commercehub.watershed.pump.model.PreviewSettings
import com.commercehub.watershed.pump.model.PumpSettings
import com.commercehub.watershed.pump.service.JobService
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Guice
import io.dropwizard.testing.junit.ResourceTestRule
import org.junit.Rule
import spock.lang.Shared
import spock.lang.Specification

import javax.ws.rs.ProcessingException
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response


class PumpResourceSpec extends Specification {
    @Shared
    JobService jobService

    @Shared
    PumpResource pumpResource = new PumpResource()

    @Shared
    ObjectMapper objectMapper

    @Rule
    ResourceTestRule resources = new ResourceTestRule.Builder()
            .addResource(pumpResource)
            .build()

    def setup() {
        GuiceBridge.setOverrideInjector(Guice.createInjector(new PumpGuiceModule()))
        objectMapper = GuiceBridge.getInjector().getInstance(ObjectMapper)

        jobService = Mock(JobService)
        pumpResource.jobService = jobService
        pumpResource.objectMapper = objectMapper
    }

    /* *************** */
    /*  POST /preview  */
    /* *************** */

    def "POST /preview is successful"(){
        setup:
        PreviewSettings previewSettings = new PreviewSettings(queryIn: "select * from foo", previewCount: 3)
        String previewSettingsJSON = objectMapper.writeValueAsString(previewSettings)

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs/preview")
                .request()
                .post(Entity.entity(previewSettingsJSON, MediaType.APPLICATION_JSON))

        then:
        1 * jobService.getJobPreview(previewSettings) >> new JobPreview(200, [["field1": "value1"], ["field2": "value2"], ["field2": "value2"]])
        response.status == 200
    }

    def "POST /preview fails validation for queryIn"(){
        setup:
        PreviewSettings previewSettings = new PreviewSettings(queryIn: null, previewCount: 3)
        String previewSettingsJSON = objectMapper.writeValueAsString(previewSettings)

        when:
        resources.jerseyTest.client()
                .target("/jobs/preview")
                .request()
                .post(Entity.entity(previewSettingsJSON, MediaType.APPLICATION_JSON))

        then:
        thrown(ProcessingException)
        0 * jobService.getJobPreview(_)
    }

    def "POST /preview fails validation for previewCount"(){
        setup:
        PreviewSettings previewSettings = new PreviewSettings(queryIn: "select * from foo", previewCount: -1)
        String previewSettingsJSON = objectMapper.writeValueAsString(previewSettings)

        when:
        resources.jerseyTest.client()
                .target("/jobs/preview")
                .request()
                .post(Entity.entity(previewSettingsJSON, MediaType.APPLICATION_JSON))

        then:
        thrown(ProcessingException)
        0 * jobService.getJobPreview(_)
    }


    /* *************** */
    /*   POST /jobs    */
    /* *************** */

    def "POST /jobs is successful"(){
        setup:
        PumpSettings pumpSettings = new PumpSettings(queryIn: "select * from foo", streamOut: "MyStream")
        String pumpSettingsJSON = objectMapper.writeValueAsString(pumpSettings)

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs")
                .request()
                .post(Entity.entity(pumpSettingsJSON, MediaType.APPLICATION_JSON))

        then:
        1 * jobService.enqueueJob(pumpSettings) >> new Job(UUID.randomUUID().toString(), pumpSettings)
        response.status == 200
    }

    def "POST /jobs fails validation for queryIn"(){
        setup:
        PumpSettings pumpSettings = new PumpSettings(queryIn: null, streamOut: "MyStream")
        String pumpSettingsJSON = objectMapper.writeValueAsString(pumpSettings)

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs")
                .request()
                .post(Entity.entity(pumpSettingsJSON, MediaType.APPLICATION_JSON))

        then:
        thrown(ProcessingException)
        0 * jobService.queueJob(_)
    }

    def "POST /jobs fails validation for streamOut"(){
        setup:
        PumpSettings pumpSettings = new PumpSettings(queryIn: "select * from foo", streamOut: null)
        String pumpSettingsJSON = objectMapper.writeValueAsString(pumpSettings)

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs")
                .request()
                .post(Entity.entity(pumpSettingsJSON, MediaType.APPLICATION_JSON))

        then:
        thrown(ProcessingException)
        0 * jobService.queueJob(_)
    }

    /* ************************ */
    /*   GET /jobs/{job_id}     */
    /* ************************ */

    def "GET /jobs/{job_id} is successful"(){
        setup:
        String jobId = UUID.randomUUID().toString()
        Job job = new Job(jobId, new PumpSettings(queryIn: "select * from foo", streamOut: "MyStream"))

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs")
                .path(jobId)
                .request()
                .get()

        then:
        1 * jobService.getJob(jobId) >> job
        response.status == 200
        response.readEntity(String.class) == objectMapper.writeValueAsString(job)
    }

    def "GET /jobs/{job_id} returns null of not found"(){
        setup:
        String jobId = UUID.randomUUID().toString()

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs")
                .path(jobId)
                .request()
                .get()

        then:
        1 * jobService.getJob(jobId) >> null
        response.statusInfo == Response.Status.NOT_FOUND
    }


    /* *************** */
    /*   GET /jobs     */
    /* *************** */

    def "GET /jobs is successful"(){
        setup:
        String jobId = UUID.randomUUID().toString()
        Job job = new Job(jobId, new PumpSettings(queryIn: "select * from foo", streamOut: "MyStream"))

        when:
        Response response = resources.jerseyTest.client()
                .target("/jobs")
                .request()
                .get()

        then:
        1 * jobService.getAllJobs() >> [job]
        response.status == 200
        response.readEntity(String.class) == objectMapper.writeValueAsString([job])
    }
}
