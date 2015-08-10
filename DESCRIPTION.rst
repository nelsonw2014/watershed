Watershed
=========
Without using any costly database, this solution complements Amazon Kinesis with the following capabilities:

* Long-term archival of records.
* Making both current and archived records accessible to SQL-based exploration and analysis.
* Replay of archived records:
    * which supports key-value compaction so only the last record for a key is replayed.
    * which supports bounded replay (one needn’t replay the full archive).
    * which supports filtered replay (only replay records matching some criteria).
    * which supports annotating records as they are replayed in order to alter consumer behavior, such as to force overwrite.
    * which, with consumer cooperation, provides some definition of eventual consistency with respect to records that arrive on a stream concurrently with a replay operation, without requiring this solution to mediate the flow of the stream.

Getting Started
---------------
1. Copy the config from `watershed/conf/defaults.json` and modify it to fit your needs.
2. Execute the command in a shell:
    * `python3 -m watershed all -c <config_file> -k <private_key>`

Reference documentation
-----------------------
See https://github.com/commercehub-oss/watershed
