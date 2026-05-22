---
name: spark-data-processing
description: Use when developing Spark jobs, writing PySpark code, or processing large-scale data using Spark/Flink in data platform projects.
---

# Spark Data Processing Skill

## Overview

Develop scalable Spark jobs for PB级 data processing. Core principle: prefer DataFrame APIs over RDDs, use structured streaming for real-time processing, and always handle data quality at ingestion.

## When to Use

**Trigger when you hear:**

- "写一个 Spark Job"
- "处理 TB 级数据"
- "PySpark 数据清洗"
- "Spark 特征工程"
- "Spark 实时流处理"
- "Flink 作业"
- "Hive 表读取"

**Do NOT use when:**

- Simple single-file transformations (use pandas instead)
- Already have approved Spark job and need debugging help (use systematic-debugging)

## Core Pattern: Spark Job Development

### 1. Data Ingestion

```python
# ✅ Good: 使用 DataFrame API，支持 schema 演进和优化
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("data-processing-job") \
    .enableHiveSupport() \
    .getOrCreate()

# 显式定义 schema（避免运行时推断）
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("value", IntegerType(), True)
])

df = spark.read \
    .format("json") \
    .schema(schema) \
    .option("compression", "snappy") \
    .load("hdfs:///data/events/*.json")
```

### 2. Data Quality Gates

```python
# ✅ Always: 数据质量检查
from pyspark.sql.functions import col, count, when

def validate_data(df, required_columns):
    """数据验证关卡"""
    # 检查必需列存在
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    # 检查空值率
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in required_columns
    ]).collect()[0].asDict()

    for col_name, null_count in null_counts.items():
        null_rate = null_count / df.count()
        if null_rate > 0.5:
            raise ValueError(f"Column {col_name} has {null_rate*100}% null rate")

    return df

df = validate_data(df, ["user_id", "event_type", "timestamp"])
```

### 3. Feature Engineering

```python
# ✅ Good: 使用 Spark ML Feature Transformers
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

# 类别特征编码
indexer = StringIndexer(inputCol="category", outputCol="category_index")

# 特征组装
assembler = VectorAssembler(
    inputCols=["category_index", "value1", "value2"],
    outputCol="features"
)

# 特征标准化
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withMean=True,
    withStd=True
)

# 构建 Pipeline
pipeline = Pipeline(stages=[indexer, assembler, scaler])
model = pipeline.fit(df)
df_transformed = model.transform(df)
```

### 4. Window Functions for Time Series

```python
# ✅ Good: 使用 Window 函数进行时序分析
from pyspark.sql import Window
from pyspark.sql.functions import col, lag, sum, avg, count

window_spec = Window \
    .partitionBy("user_id") \
    .orderBy("timestamp") \
    .rowsBetween(-7, 0)  # 最近7天

# 计算7天滚动指标
df_with_metrics = df.withColumn(
    "rolling_7d_value",
    sum("value").over(window_spec)
).withColumn(
    "rolling_7d_avg",
    avg("value").over(window_spec)
).withColumn(
    "event_count_7d",
    count("event_type").over(window_spec)
)
```

### 5. Structured Streaming

```python
# ✅ Good: 结构化流处理
from pyspark.sql.functions import window

# 读取 Kafka 流
input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .load()

# 解析 JSON
events_df = input_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 窗口聚合
aggregated_df = events_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "event_type"
    ) \
    .count()

# 输出到 Kafka
query = aggregated_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated-events") \
    .option("checkpointLocation", "hdfs:///checkpoint/events") \
    .outputMode("complete") \
    .start()
```

## Common Mistakes

| Mistake | Why It Fails | Fix |
|---------|--------------|-----|
| Using RDD instead of DataFrame | No Catalyst optimizer, slower execution | Always prefer `df.select()`, `df.filter()` |
| No schema definition | Spark infers schema at runtime (slow + unreliable) | Define schema explicitly at read time |
| Missing data validation | Dirty data causes downstream model failures | Always add quality gates |
| No partition strategy | Skewed data causes OOM on some executors | Repartition by key before joins |
| Not using broadcast for small tables | Large shuffle for simple joins | Add `.hint("broadcast")` or `broadcast(df)` |
| Checkpoint not set for streaming | Job failure = no recovery | Always set checkpointLocation |
| collect() on large dataset | Driver OOM | Use `.show()`, `.write()`, or sampling first |

## Performance Tips

1. **广播小表**：`.hint("broadcast")` for joins with tables < 10MB
2. **避免重复计算**：`.cache()` for datasets used multiple times
3. **适当分区**：`.repartition(n)` before heavy shuffles
4. **列裁剪**：`.select("only", "needed", "cols")` to reduce data volume
5. **谓词下推**：Filter early, filter often

## Red Flags - STOP

- `df.collect()` on data without size check
- No schema at read time (letting Spark infer)
- No checkpoint for streaming jobs
- `coalesce(1)` writing to single file
- No error handling on external data sources

**All of these mean: Your job will fail in production.**
