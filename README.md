# spark-course

Companion code for the self-study book *Apache Spark: from fundamentals to a complete data pipeline*.
The book holds the theory, diagrams and exercise statements; this repository holds all runnable code.

## Setup

Requirements: Java 17 or 21, Python 3.10+.

```bash
python -m venv spark-env
source spark-env/bin/activate        # Windows: spark-env\Scripts\activate
pip install -r requirements.txt
python data/generate_data.py         # creates the VietMart dataset (fixed seed)
```

## Layout

| Path | Content |
|---|---|
| `common/` | `get_spark()`, schemas, loaders and shared transformations |
| `data/generate_data.py` | Synthetic dataset generator (standard library only, `random.seed(42)`) |
| `chNN_<topic>/examples.py` | Full versions of the snippets shown in the book, one function per section |
| `chNN_<topic>/solutions.py` | Exercise solutions, one function per exercise (`ex_5_2` = exercise 5.2) |
| `ch12_deployment/jobs`, `tests` | A standalone `spark-submit` job and its `pytest` unit tests |
| `ch13_capstone/pipeline.py` | Bronze / silver / gold pipeline |
| `run_all.py` | Smoke test that runs every module |

Chapter 1 is theory only and has no folder.

## Running

Always run from the repository root.

```bash
python -m ch05_dataframe.examples            # every example of chapter 5
python -m ch05_dataframe.examples 5.4        # only section 5.4
python -m ch05_dataframe.solutions 5.2 5.3   # selected exercises
pytest ch12_deployment
spark-submit --master "local[*]" ch12_deployment/jobs/monthly_revenue.py --input data --output output/gold
python -m ch13_capstone.pipeline
python run_all.py                            # everything (takes a few minutes)
```

The Spark UI is available at http://localhost:4040 while a program is running.
Outputs go to `output/` and `lake/` (both git-ignored).

## Notes

- Tested with Spark 4.3.0, Python 3.13 and Java 21.
- `ch10_streaming/kafka_source.py` and `ch12_deployment/spark_connect_client.py` are reference
  snippets: they need a Kafka broker / a Spark Connect server and are not part of `run_all.py`.
- Try each exercise yourself before opening `solutions.py`.
