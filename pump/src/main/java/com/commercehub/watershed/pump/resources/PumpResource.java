package com.commercehub.watershed.pump.resources;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.service.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;


@Path("/pump")
@Produces(MediaType.APPLICATION_JSON)
public class PumpResource {
final Logger log = LoggerFactory.getLogger(PumpResource.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    JobService jobService;

    @Inject
    public PumpResource() {
        log.info("Creating a new PumpResource.");
    }

    @Path("/status/{job_id}")
    @GET
    public Response getJob(@PathParam("job_id") String jobId) throws IOException{
        Job job = jobService.getJob(jobId);
        if(job == null){
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        String response = objectMapper.writeValueAsString(job);
        return Response.ok().entity(response).build();
    }

    @Path("/status")
    @GET
    public Response getAllJobs() throws IOException{
        Collection<Job> jobs = jobService.getAllJobs();

        String response = objectMapper.writeValueAsString(jobs);
        return Response.ok().entity(response).build();
    }

    @Path("/queue")
    @POST
    public Response queueJob(@Valid PumpSettings pumpSettings) throws IOException{
        Job job = jobService.queueJob(pumpSettings);

        try {
            String response = objectMapper.writeValueAsString(job);
            return Response.ok().entity(response).build();
        }catch(Exception ex){
            log.error(ex.getMessage(), ex);
        }

        return Response.ok().build();
    }

    @Path("/preview")
    @POST
    public Response previewJob(@Valid PreviewSettings previewSettings) throws IOException{
        JobPreview jobPreview = jobService.getJobPreview(previewSettings);

        String response = objectMapper.writeValueAsString(jobPreview);
        return Response.ok().entity(response).build();
    }
}
