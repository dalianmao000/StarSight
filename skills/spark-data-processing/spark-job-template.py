#!/usr/bin/env python3
"""
Spark Job Template

标准化 Spark Job 模板，包含数据读取、质量验证、特征工程、输出的完整流程。
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, avg, stddev,
    window, lag, lead, row_number
)
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import logging
import argparse

# ========================================
# 配置日志
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("spark-job-template")


# ========================================
# 参数解析
# ========================================

def parse_args():
    parser = argparse.ArgumentParser(description='Spark Job Template')
    parser.add_argument('--input', required=True, help='Input path')
    parser.add_argument('--output', required=True, help='Output path')
    parser.add_argument('--job-date', required=False, help='Job execution date (YYYY-MM-DD)')
    parser.add_argument('--env', default='prod', choices=['dev', 'prod'], help='Environment')
    return parser.parse_args()


# ========================================
# 数据模型
# ========================================

def get_event_schema():
    """定义事件数据 schema"""
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_value", DoubleType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("device_type", StringType(), True),
        StructField("os_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("app_version", StringType(), True)
    ])


# ========================================
# 数据质量验证
# ========================================

class DataQualityValidator:
    """数据质量验证器"""

    def __init__(self, df):
        self.df = df
        self.errors = []

    def check_required_columns(self, required_columns):
        """检查必需列"""
        missing = [c for c in required_columns if c not in self.df.columns]
        if missing:
            self.errors.append(f"Missing columns: {missing}")
        return len(missing) == 0

    def check_null_rate(self, column, max_null_rate=0.5):
        """检查空值率"""
        total = self.df.count()
        if total == 0:
            self.errors.append("DataFrame is empty")
            return False

        null_count = self.df.filter(col(column).isNull()).count()
        null_rate = null_count / total

        if null_rate > max_null_rate:
            self.errors.append(f"Column {column} null rate {null_rate:.2%} > {max_null_rate:.2%}")
            return False
        return True

    def check_duplicate_id(self, id_column):
        """检查重复 ID"""
        duplicate_count = self.df.groupBy(id_column).count().filter("count > 1").count()
        if duplicate_count > 0:
            self.errors.append(f"Found {duplicate_count} duplicate {id_column}")
            return False
        return True

    def check_value_range(self, column, min_val, max_val):
        """检查值范围"""
        out_of_range = self.df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()

        if out_of_range > 0:
            self.errors.append(f"Column {column} has {out_of_range} out-of-range values")
            return False
        return True

    def validate(self) -> bool:
        """执行所有验证"""
        logger.info("Starting data quality validation...")

        # 必需列检查
        self.check_required_columns(["event_id", "user_id", "timestamp"])

        # 关键列空值率检查
        for col_name in ["user_id", "event_type", "timestamp"]:
            self.check_null_rate(col_name, max_null_rate=0.1)

        # 重复 ID 检查
        self.check_duplicate_id("event_id")

        if self.errors:
            logger.error(f"Validation failed: {'; '.join(self.errors)}")
            return False

        logger.info("Data quality validation passed")
        return True


# ========================================
# 特征工程
# ========================================

def compute_user_features(df):
    """计算用户级别特征"""

    # 用户行为统计
    user_stats = df.groupBy("user_id").agg(
        count("event_id").alias("event_count"),
        spark_sum("event_value").alias("total_value"),
        avg("event_value").alias("avg_value"),
        count("DISTINCT session_id").alias("session_count")
    )

    # 最近活跃时间
    window_latest = Window.partitionBy("user_id").orderBy(col("timestamp").desc())
    df_with_rank = df.withColumn("rank", row_number().over(window_latest))

    latest_active = df_with_rank.filter(col("rank") == 1).select(
        "user_id",
        col("timestamp").alias("latest_active_time")
    )

    # 7天滚动指标
    window_7d = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-7, 0)
    df_rolling = df.withColumn(
        "rolling_7d_value",
        spark_sum("event_value").over(window_7d)
    ).withColumn(
        "rolling_7d_count",
        count("event_id").over(window_7d)
    )

    # 合并特征
    user_features = user_stats.join(latest_active, "user_id")

    return user_features


def compute_session_features(df):
    """计算 Session 级别特征"""

    # Session 统计
    session_stats = df.groupBy("session_id").agg(
        count("event_id").alias("events_in_session"),
        spark_sum("event_value").alias("session_value"),
        count("DISTINCT event_type").alias("distinct_events"),
        count("DISTINCT user_id").alias("user_count")
    )

    # Session 时长（开始-结束）
    from pyspark.sql.functions import min as spark_min, max as spark_max

    session_duration = df.groupBy("session_id").agg(
        spark_min("timestamp").alias("session_start"),
        spark_max("timestamp").alias("session_end")
    )

    from pyspark.sql.functions import unix_timestamp

    session_duration = session_duration.withColumn(
        "session_duration_seconds",
        (unix_timestamp("session_end") - unix_timestamp("session_start")).cast("double")
    )

    return session_stats.join(session_duration, "session_id")


# ========================================
# 主 Job
# ========================================

def main():
    args = parse_args()
    logger.info(f"Starting job with args: {args}")

    # 1. 创建 Spark Session
    spark = SparkSession.builder \
        .appName("DataProcessingJob") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    logger.info("Spark session created")

    # 2. 读取数据
    logger.info(f"Reading data from {args.input}")

    # 尝试读取，schema 未知时使用 auto
    try:
        # 尝试 Parquet（推荐）
        df = spark.read.parquet(args.input)
        logger.info(f"Read {df.count()} rows from Parquet")
    except Exception as e:
        logger.warning(f"Parquet read failed: {e}, trying JSON")
        try:
            df = spark.read.json(args.input, schema=get_event_schema())
            logger.info(f"Read {df.count()} rows from JSON")
        except Exception as e2:
            logger.warning(f"JSON read failed: {e2}, reading as text")
            # 最后的降级方案
            df = spark.read.text(args.input)
            logger.info(f"Read {df.count()} rows from text")

    # 3. 数据质量验证
    validator = DataQualityValidator(df)
    if not validator.validate():
        raise ValueError(f"Data quality validation failed: {validator.errors}")

    # 4. 特征工程
    logger.info("Computing features...")

    # 用户特征
    user_features = compute_user_features(df)
    logger.info(f"Computed {user_features.count()} user features")

    # Session 特征
    session_features = compute_session_features(df)
    logger.info(f"Computed {session_features.count()} session features")

    # 5. 写入输出
    logger.info(f"Writing output to {args.output}")

    # 输出为 Parquet（带压缩）
    user_features.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("user_id") \
        .save(f"{args.output}/user_features")

    session_features.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("session_id") \
        .save(f"{args.output}/session_features")

    logger.info("Job completed successfully")

    # 6. 关闭 Spark
    spark.stop()


if __name__ == "__main__":
    main()
