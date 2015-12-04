package com.commercehub.watershed.pump.model;

import com.amazonaws.services.kinesis.model.Record;

public class PumpRecord {
    Record kinesisRecord;
    ResultRow drillRow;

    public PumpRecord(Record kinesisRecord, ResultRow drillRow) {
        this.kinesisRecord = kinesisRecord;
        this.drillRow = drillRow;
    }

    public Record getKinesisRecord() {
        return kinesisRecord;
    }

    public ResultRow getDrillRow() {
        return drillRow;
    }
}
