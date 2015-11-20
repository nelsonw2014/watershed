package com.commercehub.watershed.pump.respositories;


import com.commercehub.watershed.pump.model.JobPreview;
import com.commercehub.watershed.pump.model.PreviewSettings;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DrillRepository implements QueryableRepository {
    private static final Logger log = LoggerFactory.getLogger(DrillRepository.class);

    @Inject
    private Database database;

    @Override
    public JobPreview getJobPreview(PreviewSettings previewSettings) {
        Connection connection = database.getConnectionProvider().get();

        String countSql = "SELECT count(*) as total FROM (" + previewSettings.getQueryIn() + ")";
        Integer count = null;
        List<Map<String, String>> rows = null;
        try{
            ResultSet resultSet = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(countSql);
            resultSet.setFetchSize(1);
            resultSet.next();
            count = resultSet.getInt("total");

            resultSet = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(previewSettings.getQueryIn());
            resultSet.setFetchSize(previewSettings.getPreviewCount());

            rows = resultSetToArrayList(resultSet, previewSettings.getPreviewCount());
        }
        catch(SQLException ex){
            log.error("Unable to get job preview.", ex);
        }
        finally {
            try {
                connection.close();
            }
            catch (Exception e) {
                log.warn("Failed to close database connection.", e);
            }
        }

        return new JobPreview(count, rows);
    }

    private List<Map<String, String>> resultSetToArrayList(ResultSet resultSet, Integer rowLimit) throws SQLException{
        ResultSetMetaData md = resultSet.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, String>> list = new ArrayList<>();

        while (resultSet.next() && resultSet.getRow() < rowLimit){
            Map<String, String> row = new HashMap<>();
            for(int i = 1; i <= columns; i++){
                if(md.getColumnType(i) == Types.BOOLEAN){
                    row.put(md.getColumnName(i), String.valueOf(resultSet.getBoolean(i)));
                    continue;
                }

                row.put(md.getColumnName(i), new String(resultSet.getBytes(i)));
            }

            list.add(row);
        }

        return list;
    }
}
