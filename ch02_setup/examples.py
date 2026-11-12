"""Chapter 2 - first Spark program.  Run: python -m ch02_setup.examples"""
from common.session import get_spark


def main() -> None:
    spark = get_spark("ch02-first-app")
    print("Spark version:", spark.version)
    print("Master:", spark.sparkContext.master)

    df = spark.range(1, 6)          # one column `id` with values 1..5
    df.show()


if __name__ == "__main__":
    main()
