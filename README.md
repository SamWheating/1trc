# 1trc

[1 Trillion Row Challenge](https://github.com/coiled/1trc) in Spark.

Mostly set up using [sparkProjectTemplate.g8](https://github.com/holdenk/sparkProjectTemplate.g8).

### Running this code locally

(Requires a working Java installation, tested with OpenJDK 11)

I've inlcuded 2 parquet files (~24 MB each, sorry) so that this job can be run fully locally.

Also included is [Paul Phillip's SBT runner](https://github.com/dwijnand/sbt-extras), so you should be able to just run the job like so:

```bash
./sbt "run sample_data/measurements-*.parquet ./output"
```
And selecting the local application when prompted.

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
