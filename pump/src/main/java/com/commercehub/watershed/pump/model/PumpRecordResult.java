package com.commercehub.watershed.pump.model;

import com.amazonaws.services.kinesis.producer.UserRecordResult;

/**
 * Holds results for the KinesisRecord and the row that was pulled from Drill.
 */
public class PumpRecordResult {
    private UserRecordResult userRecordResult;
    private ResultRow resultRow;

    public PumpRecordResult(UserRecordResult userRecordResult, ResultRow resultRow) {
        this.userRecordResult = userRecordResult;
        this.resultRow = resultRow;
    }

    /**
     * Get the Kinesis UserRecordResult
     * @return UserRecordResult
     */
    public UserRecordResult getUserRecordResult() {
        return userRecordResult;
    }

    /**
     * Get the row that was pulled from Drill.
     * @return
     */
    public ResultRow getResultRow() {
        return resultRow;
    }
}
