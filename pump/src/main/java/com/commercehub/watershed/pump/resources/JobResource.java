package com.commercehub.watershed.pump.resources;

import com.commercehub.watershed.pump.model.Job;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;


@Path("/job")
@Produces(MediaType.APPLICATION_JSON)
public class JobResource {
final Logger log = LoggerFactory.getLogger(JobResource.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    public JobResource() {
        log.info("Creating a new JobResource.");
    }

    @Path("/{job_id}")
    @GET
    public Response getJob(@PathParam("job_id") String jobId) throws IOException{
        Job job = new Job();

        String response = objectMapper.writeValueAsString(job);
        return Response.ok().entity(response).build();
    }
}
