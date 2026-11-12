"""Chapter 11 solutions.  Run: python -m ch11_mllib.solutions [11.1 11.2 11.3]"""
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from ch11_mllib.examples import split
from common.data import completed_orders, load_items_with_total, load_orders, order_totals
from common.session import run_cli


def rfm_table(spark):
    reference_date = F.to_date(F.lit("2026-01-01"))
    return (
        completed_orders(load_orders(spark))
        .filter(F.col("customer_id") != 1)
        .join(order_totals(load_items_with_total(spark)), "order_id")
        .groupBy("customer_id")
        .agg(
            F.datediff(reference_date, F.max("order_date")).alias("recency"),
            F.count("*").alias("frequency"),
            F.sum("order_value").alias("monetary"),
        )
    )


def ex_11_1(spark):
    rfm_table(spark).describe("recency", "frequency", "monetary").show()


def ex_11_2(spark):
    rfm = rfm_table(spark)
    # Scale first: monetary is in millions and would dominate Euclidean distance.
    prep = Pipeline(stages=[
        VectorAssembler(inputCols=["recency", "frequency", "monetary"], outputCol="raw"),
        StandardScaler(inputCol="raw", outputCol="features", withMean=True),
    ]).fit(rfm)
    rfm_vec = prep.transform(rfm).cache()

    evaluator = ClusteringEvaluator(featuresCol="features")      # silhouette
    results = {}
    for k in [3, 4, 5]:
        model = KMeans(k=k, seed=42, featuresCol="features").fit(rfm_vec)
        results[k] = (evaluator.evaluate(model.transform(rfm_vec)), model)
        print(f"k={k} silhouette={results[k][0]:.3f}")

    best_k = max(results, key=lambda k: results[k][0])
    clustered = results[best_k][1].transform(rfm_vec)
    (clustered.groupBy("prediction")
              .agg(F.count("*").alias("num_customers"),
                   F.round(F.avg("recency")).alias("avg_recency"),
                   F.round(F.avg("frequency"), 1).alias("avg_frequency"),
                   F.round(F.avg("monetary")).alias("avg_monetary"))
              .orderBy("prediction").show())
    rfm_vec.unpersist()


def ex_11_3(spark):
    train, test = split(spark)
    features = ["pm_idx", "num_lines", "order_value", "order_hour"]
    # Trees need neither scaling nor one-hot encoding.
    pipeline = Pipeline(stages=[
        StringIndexer(inputCol="payment_method", outputCol="pm_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=features, outputCol="features"),
        RandomForestClassifier(featuresCol="features", labelCol="label",
                               numTrees=50, maxDepth=5, seed=42),
    ])
    model = pipeline.fit(train)
    auc = BinaryClassificationEvaluator(labelCol="label").evaluate(model.transform(test))
    print("Random Forest AUC:", round(auc, 3))

    importances = model.stages[-1].featureImportances.toArray()
    for name, score in sorted(zip(features, importances), key=lambda x: -x[1]):
        print(f"{name:15s} {score:.3f}")


if __name__ == "__main__":
    run_cli({"11.1": ex_11_1, "11.2": ex_11_2, "11.3": ex_11_3}, "ch11-solutions")
