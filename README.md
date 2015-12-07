# watershed
Data streams drain into a data reservoir so they can be used later or combined together.

Without using any costly database, this solution complements
[Amazon Kinesis Streams](http://aws.amazon.com/kinesis/) with the following capabilities:
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
* [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/) will be supported as the reference implementation for stream archival.
* Message replay capability is well under way, leveraging the [Kinesis Producer Library](http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-kpl.html); see the _pump_ directory.
* Refer to the Issues list for more details.

## Assumptions and Applicability Constraints
* This is mostly an integration project, light on actual software.
* This will probably be more of an ephemeral tool than a service, but the
archival portion will have to run at least once every 24 hours (the Kinesis
record expiration time) in order to not miss any records.
* The initial implementation might only support full functionality with JSON records, but further
contributions should be able to remove that as a requirement. Avro and Parquet formats are of future interest.
* Data and cluster security is currently left to the user. Users will have varied needs for security around data, networks, authentication, authorization. The goal is to enable users to apply that security out-of-band or via flexible configuration hooks, so the project needn't support every possible combination. Approriate use of restricted IAM Users and Roles is encouraged, as is the use of truly private SSH keys.

## Technical Goals
* Configure [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/) or an equivalent process to archive
blocks of Amazon Kinesis records to [Amazon S3](http://aws.amazon.com/s3/).
* Launch an [Amazon EMR](http://aws.amazon.com/elasticmapreduce/) cluster
including the Hive application.
* Deploy [Apache Drill](http://drill.apache.org/) to the cluster.
* Configure Apache Drill to read archived records from Amazon S3.
* Configure [Amazon EMR Hive](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-hive.html)
to expose an Amazon Kinesis stream as an externally-stored table.
* Configure the Amazon EMR Hive
[Metastore](https://drill.apache.org/docs/hive-storage-plugin/) for consumption
by Apache Drill.
* To the greatest extent possible without storing another copy of the data,
provide a unified and de-duplicated SQL view spanning current and archived Amazon Kinesis records.
* Provide a basic UI or API to initiate search and replay operations, and monitor progress.
 
# Prerequisites
* Python 3.4 and pip3

# Getting Started
* Install watershed
    * You can install watershed for use with the command `python3 setup.py install`
         * This makes watershed executable from the commandline with `python3 -m watershed` from any location
    * If you're developing watershed, use `python3 setup.py develop`
         * This only installs dependencies. As a result, you must be in the repo directory to run watershed
         * We recommend you establish a virtual environment for development
              * `pip3 install virtualenv`
              * `virtualenv <env_name>`
              * `source <env_name>/bin/activate`
              * You can now run any setup or applications as you would normally
              * More info on virtualenv can be found [here](https://virtualenv.pypa.io/en/latest/userguide.html)
* Create a config file
    * Make a copy of `conf/defaults.json` and edit the copy
* Run `python3 -m watershed all -c <config-file> -k <private-key-file>`
    * Ctrl-C will stop the forwarding at any time.
    * `-t` will terminate the cluster on closing of port forwarding
* For additional information, use the `-h` argument without any arguments or with any subcommand:
    * upload-resources (u)
    * launch-cluster (l)
    * wait-for-cluster (w)
    * forward-local-ports (f)
    * configure-streams (c)
    * terminate-cluster (t)
    * all (all)
* To run the acceptance test, run `python3 setup.py nosetests --tests tests/acceptance_test.py`
    * You need two environment variables to be set for this to work:
        * `PRIVATE_KEY_FILE` should be the private key setup for your EMR configuration, i.e. `/path/to/key.pem`
        * `WATERSHED_CONFIG` should be the location of your configuration, i.e. `/path/to/defaults.json`
* To run the configuration validation on a cluster already running, run `python3 setup.py nosetests --tests tests/conf_validation_test.py`
    * You need three environment variables to be set for this to work:
        * `CLUSTER_ID` should be the Cluster ID of the cluster you want to query, i.e. `j-XXXXXXXXXXXXX`
        * `PRIVATE_KEY_FILE` should be the private key setup for your EMR configuration, i.e. `/path/to/key.pem`
        * `WATERSHED_CONFIG` should be the location of your configuration, i.e. `/path/to/defaults.json`
        
# pump
Pump gives you a way to query, modify, and re-emit data to an Amazon Kinesis Stream.

* Pump is deployed during watershed's "launch-cluster" command.


# pump client
Interact with pump via the pump client provided in the main watershed directory.

```
> python3 -m pump_client -h
Client to manage and monitor Pump.

positional arguments:
  {create-job,preview-job,get-job,get-all-jobs}
    create-job          Enqueue a job for Pump to work on.
    preview-job         Preview a job before Pump runs it.
    get-job             Retreive Job status from Pump.
    get-all-jobs        Retreive all Jobs and their statuses from Pump.

optional arguments:
  -h, --help            show this help message and exit
```

## Create Job
Submit a job to Pump - queries Drill, transforms records, emits to Kinesis stream.

```
> python3 -m pump_client create-job -h
usage: /private/projects/watershed/pump_client create-job [-h] -q QUERY -s
                                                          STREAM [-p]

optional arguments:
  -h, --help            show this help message and exit
  -q QUERY, --query QUERY
                        Specify the query Pump will use to pull records out of
                        Drill.
  -s STREAM, --stream STREAM
                        Specify the Kinesis stream that Pump will emit records
                        to.
  -p, --show-progress   Poll Pump for progress of job
```

## Preview Job
Preview a Job before running it - queries Drill and returns the first N results.

```
> python3 -m pump_client preview-job -h
usage: /private/projects/watershed/pump_client preview-job [-h] -q QUERY -n
                                                           NUM_RECORDS

optional arguments:
  -h, --help            show this help message and exit
  -q QUERY, --query QUERY
                        Specify the query Pump will use to pull records out of
                        Drill.
  -n NUM_RECORDS, --num-records NUM_RECORDS
                        Specify the number of records to return in the
                        preview.
```

## Get Job
After submitting, use this to retrieve a Job from Pump to see its status.

```
> python3 -m pump_client get-job -h
usage: /private/projects/watershed/pump_client get-job [-h] -id JOB_ID [-p]
                                                       [-t]

optional arguments:
  -h, --help            show this help message and exit
  -id JOB_ID, --job-id JOB_ID
                        Specify the Job ID to retrieve.
  -p, --show-progress   Poll Pump for progress of job
  -t, --summary-only    Only show summary information about the Job
```

## Get all Jobs
Show all statuses for Jobs that have been submitted during the session.

```
> python3 -m pump_client get-all-jobs -h
usage: /private/projects/watershed/pump_client get-all-jobs [-h] [-t]

optional arguments:
  -h, --help          show this help message and exit
  -t, --summary-only  Only show summary information about the Job
```