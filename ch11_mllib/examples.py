"""Chapter 11 - MLlib: predicting cancelled orders.  Run: python -m ch11_mllib.examples"""
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as F

from common.data import load_order_items, load_orders, out_path
from common.session import run_cli


def order_features(spark):
    """One row per order with features known at order time and a binary label."""
    orders, items = load_orders(spark), load_order_items(spark)
    return (
        items.groupBy("order_id").agg(
            F.count("*").alias("num_lines"),
            F.sum(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))).alias("order_value"),
        )
        .join(orders.filter(F.col("status") != "SHIPPING"), "order_id")   # outcome still unknown
        .withColumn("order_hour", F.hour("order_date"))
        .withColumn("label", (F.col("status") == "CANCELLED").cast("double"))
    )


def split(spark):
    return order_features(spark).randomSplit([0.8, 0.2], seed=42)


def build_pipeline() -> Pipeline:
    return Pipeline(stages=[
        StringIndexer(inputCol="payment_method", outputCol="pm_idx", handleInvalid="keep"),
        OneHotEncoder(inputCols=["pm_idx"], outputCols=["pm_vec"]),
        VectorAssembler(inputCols=["pm_vec", "num_lines", "order_value", "order_hour"], outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label"),
    ])


def train_and_evaluate(spark):
    train, test = split(spark)
    model = build_pipeline().fit(train)                # fit on the training set only
    predictions = model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(predictions)
    print("Test AUC:", round(auc, 3))
    predictions.select("order_id", "payment_method", "num_lines", "label",
                       "probability", "prediction").show(5, truncate=False)

    model.write().overwrite().save(out_path("cancel_model"))
    reloaded = PipelineModel.load(out_path("cancel_model"))
    print("Reloaded model scored rows:", reloaded.transform(test).count())


def cross_validation(spark):
    train, _ = split(spark)
    pipeline = build_pipeline()
    lr = pipeline.getStages()[-1]
    grid = ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.1]).build()
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=BinaryClassificationEvaluator(labelCol="label"),
        numFolds=3, seed=42, parallelism=2,
    )
    cv_model = cv.fit(train)
    print("Mean AUC per configuration:", [round(float(m), 3) for m in cv_model.avgMetrics])


if __name__ == "__main__":
    run_cli({"11.2": train_and_evaluate, "11.3": cross_validation}, "ch11-examples")
