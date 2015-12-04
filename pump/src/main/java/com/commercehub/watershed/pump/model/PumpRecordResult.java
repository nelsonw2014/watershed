package com.commercehub.watershed.pump.model;

import com.amazonaws.services.kinesis.producer.UserRecordResult;

public class PumpRecordResult {
    private UserRecordResult userRecordResult;
    private ResultRow resultRow;

    public PumpRecordResult(UserRecordResult userRecordResult, ResultRow resultRow) {
        this.userRecordResult = userRecordResult;
        this.resultRow = resultRow;
    }

    public UserRecordResult getUserRecordResult() {
        return userRecordResult;
    }

    public ResultRow getResultRow() {
        return resultRow;
    }
}
