package com.commercehub.watershed.pump.respositories;


import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;

public interface QueryableRepository {
    JobPreview getJobPreview(PreviewSettings previewSettings);
}
