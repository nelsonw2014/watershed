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
import java.sql.SQLException;
import java.util.Collection;


@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
public class PumpResource {

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private JobService jobService;

    @Path("/{job_id}")
    @GET
    public Response getJob(@PathParam("job_id") String jobId) throws IOException{
        Job job = jobService.getJob(jobId);
        if(job == null){
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        String response = objectMapper.writeValueAsString(job);
        return Response.ok().entity(response).build();
    }

    @GET
    public Response getAllJobs() throws IOException{
        Collection<Job> jobs = jobService.getAllJobs();

        String response = objectMapper.writeValueAsString(jobs);
        return Response.ok().entity(response).build();
    }

    @POST
    public Response enqueueJob(@Valid PumpSettings pumpSettings) throws IOException{
        Job job = jobService.enqueueJob(pumpSettings);

        String response = objectMapper.writeValueAsString(job);
        return Response.ok().entity(response).build();
    }

    @Path("/preview")
    @POST
    public Response previewJob(@Valid PreviewSettings previewSettings) throws IOException, SQLException{
        JobPreview jobPreview = jobService.getJobPreview(previewSettings);

        String response = objectMapper.writeValueAsString(jobPreview);
        return Response.ok().entity(response).build();
    }
}
