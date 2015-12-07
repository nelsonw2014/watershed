package com.commercehub.watershed.pump.model;

import com.amazonaws.services.kinesis.model.Record;

/**
 * Holds both a KinesisRecord and the ResultRow retrieved from Drill.
 */
public class PumpRecord {
    Record kinesisRecord;
    ResultRow drillRow;

    public PumpRecord(Record kinesisRecord, ResultRow drillRow) {
        this.kinesisRecord = kinesisRecord;
        this.drillRow = drillRow;
    }

    /**
     * Retrieve the Kinesis record
     * @return Record
     */
    public Record getKinesisRecord() {
        return kinesisRecord;
    }

    /**
     * Retrieve the Drill row
     * @return ResultRow
     */
    public ResultRow getDrillRow() {
        return drillRow;
    }
}
