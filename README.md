# 1trc

[1 Trillion Row Challenge](https://github.com/coiled/1trc) in Spark.

Also just a good exercise in writing + running a Spark job from scratch :shrug:

Mostly set up using [sparkProjectTemplate.g8](https://github.com/holdenk/sparkProjectTemplate.g8).

### Running this code locally

(Requires a working Java installation, tested with OpenJDK 11)

I've inlcuded 2 parquet files (~24 MB each, sorry) so that this job can be run fully locally.

Also included is [Paul Phillip's SBT runner](https://github.com/dwijnand/sbt-extras), so you should be able to just run the job like so:

```bash
./sbt "run sample_data/measurements-*.parquet ./output"
```
And selecting the local application (`TrillionRowChallengeLocalApp`) when prompted.

And then inspect the output like:

```bash
> head -n 5 output/*.csv
Abha,60.1,-27.1,17.996445512228416
Abidjan,65.5,-19.5,25.996608497255856
Abéché,72.5,-12.9,29.427604933154107
Accra,71.0,-14.8,26.397831693177235
Addis Ababa,56.8,-25.0,15.983908400715903
```

You can also run the tests with
```bash
./sbt test
```

### Running this job with Spark on Amazon Elastic MapReduce (EMR)

In order to properly test this job at scale, I've been running it on ephemeral YARN clusters on EMR. I've tried to make this process as reproducible as possible by hardcoding the job configuration in a submit script (`scripts/emr_submit.py`) which can probably be run with a little bit of AWS config on the user's end.

The steps to get this up and running are (approximately) as follows:
1) build a JAR with `./sbt assemble`
1) Set up an S3 bucket for storing the JAR as well as output data and logs
1) Copy said JAR to S3 bucket
1) Update the hardcoded values in the `emr_submit` script
1) Run the script (`python scripts/emr_submit`)
