package com.commercehub.watershed.pump.resources;

import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.commercehub.watershed.pump.service.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;


@Path("/pump")
@Produces(MediaType.APPLICATION_JSON)
public class PumpResource {
final Logger log = LoggerFactory.getLogger(PumpResource.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    JobService service;

    @Inject
    public PumpResource() {
        log.info("Creating a new PumpResource.");
    }

    @Path("/status/{job_id}")
    @GET
    public Response getJob(@PathParam("job_id") String jobId) throws IOException{
        Job job = service.getJob(jobId);

        String response = objectMapper.writeValueAsString(job);
        return Response.ok().entity(response).build();
    }

    @Path("/queue")
    @POST
    public Response queueJob(PumpSettings pumpSettings) throws IOException{
        Job job = service.queueJob(pumpSettings);

        String response = objectMapper.writeValueAsString(job);
        return Response.ok().entity(response).build();
    }

    @Path("/preview")
    @POST
    public Response previewJob(PreviewSettings previewSettings) throws IOException{
        JobPreview jobPreview = service.getJobPreview(previewSettings);

        String response = objectMapper.writeValueAsString(jobPreview);
        return Response.ok().entity(response).build();
    }
}
