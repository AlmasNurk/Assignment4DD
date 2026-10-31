import os, argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

ap=argparse.ArgumentParser(); ap.add_argument('--topic',required=True); args=ap.parse_args()
spark=(SparkSession.builder.appName('AITU-Almas-Assignment4').master('local[2]')
       .config('spark.sql.session.timeZone','Asia/Almaty')
       .config('spark.sql.shuffle.partitions','3').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
schema=T.StructType([T.StructField(n,t) for n,t in [
    ('event_id',T.StringType()),('timestamp',T.StringType()),('research_group',T.StringType()),
    ('job_type',T.StringType()),('duration_sec',T.DoubleType()),('status',T.StringType()),('cpu_pct',T.DoubleType())]])
raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',os.getenv('BOOTSTRAP_SERVERS','localhost:9092'))
     .option('subscribe',args.topic).option('startingOffsets','earliest').load())
events=(raw.select(F.from_json(F.col('value').cast('string'),schema).alias('e')).select('e.*')
        .withColumn('event_time',F.to_timestamp('timestamp')))
valid=events.filter(F.col('event_time').isNotNull() & (F.length(F.trim('event_id'))>0)
    & (F.length(F.trim('research_group'))>0) & F.col('job_type').isin('ml','image','text','simulation')
    & F.col('status').isin('completed','failed') & F.col('cpu_pct').between(0,100)
    & (F.col('duration_sec')>=0) & (F.col('duration_sec')<float('inf')) & ~F.isnan('duration_sec'))

clean=valid.withWatermark('event_time','10 seconds')
result=(clean.groupBy(F.window('event_time','5 seconds'),'job_type')
        .agg(F.count('*').alias('count'),F.avg('duration_sec').alias('avg_duration')))

def output(batch,batch_id):
    batch=batch.orderBy('window.start','job_type')
    batch.show(truncate=False)
    batch.coalesce(1).write.mode('overwrite').json(f'results/spark/batch_{batch_id}')


q=(result.writeStream.outputMode('update').foreachBatch(output)
   .option('checkpointLocation','results/checkpoints/'+args.topic)
   .trigger(availableNow=True).start())
q.awaitTermination()
Path('results/spark_progress.json').write_text(__import__('json').dumps(q.recentProgress,indent=2))
spark.stop()
