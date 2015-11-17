package com.commercehub.watershed.pump.respositories;


import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;

import java.sql.SQLException;

public interface QueryableRepository {
    JobPreview getJobPreview(PreviewSettings previewSettings);
}
