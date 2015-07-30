# watershed
Data streams drain into a data reservoir so they can be used later or combined together.

Without using any costly database, this solution complements
[Amazon Kinesis](http://aws.amazon.com/kinesis/) with the following capabilities:
* Long-term archival of records.
* Making both current and archived records accessible to SQL-based exploration and analysis.
* Replay of archived records:
  * which supports key-value compaction so only the last record for a key is replayed.
  * which supports bounded replay (one needn’t replay the full archive).
  * which supports filtered replay (only replay records matching some criteria).
  * which supports annotating records as they are replayed in order to alter
consumer behavior, such as to force overwrite.
  * which, with consumer cooperation, provides some definition of eventual
consistency with respect to records that arrive on a stream concurrently with a
replay operation, without requiring this solution to mediate the flow of the stream.

## Project Status
* In active development for use at CommerceHub and elsewhere. Contributions welcome.
* Capable of using SQL to search a stream archive and a live stream.
* Stream archival capability or guidance to come next.
* Message replay capability to follow.
* Refer to the Issues list for more details.

## Assumptions and Applicability Constraints
* This is mostly an integration project, light on actual software. The
[AWS CLI](http://aws.amazon.com/cli/) will be used, and is assumed to be
installed and configured.
* This will probably be more of an ephemeral tool than a service, but the
archival portion will have to run at least once every 24 hours (the Kinesis
record expiration time) in order to not miss any records.
* The initial implementation might only support JSON records, but further
contributions should be able to remove that as a requirement.
* The initial implementation might only support a single Kinesis stream, but
further contributions should be able to remove that as a requirement.
* Data and cluster security is currently left to the user. Users will have varied needs for security around data, networks, authentication, authorization. The goal is to enable users to apply that security out-of-band or via flexible configuration hooks, so the project needn't support every possible combination. 

## Technical Goals
* Configure and launch a process (TBD, there are many options) to archive
blocks of Amazon Kinesis records to [Amazon S3](http://aws.amazon.com/s3/)
before they expire, possibly via
[Amazon EMRFS](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-fs.html).
* Launch an [Amazon EMR](http://aws.amazon.com/elasticmapreduce/) cluster
including the Hive application.
* Deploy [Apache Drill](http://drill.apache.org/) to the cluster.
* Configure Apache Drill to read archived records from Amazon S3, possibly via EMRFS.
* Configure [Amazon EMR Hive](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-hive.html)
to expose an Amazon Kinesis stream as an externally-stored table.
* Configure the Amazon EMR Hive
[Metastore](https://drill.apache.org/docs/hive-storage-plugin/) for consumption
by Apache Drill.
* Configure Apache Drill to read from Amazon Kinesis via Amazon EMR Hive.
* To the greatest extent possible without storing another copy of the data,
provide a unified and de-duplicated view spanning current and archived Amazon Kinesis records.
* (TBD) Provide a basic UI or API to initiate search and replay operations, and monitor progress.
 
# Prerequisites
* Python 3.4
* Boto3 (pip3 install boto3)
* sshtunnel (pip3 install sshtunnel)

# Getting Started
* Create a config file. 
 * Make a copy of **conf/defaults.json** and edit the copy
* Run **python3 watershed.py e -c &lt;config-file&gt; -k &lt;private-key-file&gt; -s &lt;stream-config-folder&gt;**
 * Ctrl-C will stop the forwarding at any time.
 * **-t** will terminate the cluster on closing of port forwarding
* For additional information, use the **-h** argument without any arguments or with any subcommand:
 * upload-resources (u)
 * launch-cluster (l)
 * wait-for-cluster (w)
 * forward-local-ports (f)
 * create-tables (ct)
 * configure-storage-stream-archives (cs)
 * terminate-cluster (t)
 * everything (e)
