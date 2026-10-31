# Assignment 4 Distributed Stream Processing

Student: Almas Nurkaliyev  
Student ID: 242651  
Group: BDA-2401  
Instructor: Azamat Serek  
Repository: https://github.com/AlmasNurk/Assignment4DD

## Contents

- producer.py: send the ten supplied JSON events to Kafka.
- consumer.py and common.py: schema validation, duplicate handling and aggregation.
- spark_stream.py: Spark event-time windows grouped by job type.
- benchmark.py: measured Kafka loads at 10, 100 and 500 events/s.
- test_pipeline.py and verify_spark.py: calculation and Spark output checks.
- run_all.py: orchestration of the complete data-processing experiment.
- results: retained outputs from the measured experiment.
- architecture.png: architecture diagram.

The Word report is submitted separately. No document generator is needed.

## Reproduce the experiment

Prerequisites: Docker Desktop running with Linux containers, Docker Compose,
and internet access for the first image build and Spark connector download.
Run commands from the directory containing compose.yaml.

The supplied results directory contains the completed experiment. Preserve it
before running again; run_all.py refuses to overwrite an existing consumer result.
In Windows PowerShell:

```powershell
Rename-Item results results_previous
New-Item -ItemType Directory results
docker compose up -d --wait kafka
docker compose build app
docker compose run --rm app
```

If results_previous already exists, choose another unused archive name.
Wait for DONE. Logs for every stage are in results. If a stage fails, inspect
its log before retrying. The pipeline generates execution evidence, not a Word report.

To stop the local broker after collecting outputs:

```powershell
docker compose down
```

Kafka has no persistent volume in this teaching deployment. Container removal
clears its broker data. Host-mounted results remain in the project directory.

## Output and verification

consumer_results.json contains the ten-event aggregates and windows.
spark/batch_*/part-*.json contains Spark output; verify_spark.log records its
comparison with the reference calculation. tests.log records four validation tests.
performance.csv contains the measured load levels, throughput, average and p95
latency, and Python client CPU. latency_*.csv stores each measured event latency.
environment.json and run_config.json describe the environment and exact Kafka topic.

The measured experiment used 3 partitions, replication factor 1 and consumer group
aitu-python-almas. Each new run creates topics with unique suffixes.

## Measurement scope and limitations

CPU refers to the Python producer and consumer process; 100 percent represents
one fully utilized core. Kafka JVM and Spark CPU are excluded. The event field
cpu_pct is synthetic worker telemetry and is not benchmark CPU.

Latency starts immediately before send and ends after record validation and
aggregation. Each load lasts 30 seconds; there is one run per level and no separate
warm-up. The experiment does not isolate a saturated architectural bottleneck.

Python windows retain state until the finite run completes. Spark uses a 10-second
watermark and assumes unique source IDs in the fresh topic. Exactly-once processing
and single-broker fault tolerance are not claimed.

## References

- https://kafka.apache.org/38/getting-started/docker/
- https://downloads.apache.org/spark/docs/3.5.3/structured-streaming-programming-guide.html
- https://downloads.apache.org/spark/docs/3.5.3/structured-streaming-kafka-integration.html
