"""Smoke test: run every example and solution module in order.

python run_all.py            # everything
python run_all.py ch05 ch06  # only modules whose name contains one of the filters
"""
import subprocess
import sys

MODULES = [
    "ch02_setup.examples", "ch02_setup.solutions",
    "ch03_execution_model.examples", "ch03_execution_model.solutions",
    "ch04_rdd.examples", "ch04_rdd.solutions",
    "ch05_dataframe.examples", "ch05_dataframe.solutions",
    "ch06_sql_window.examples", "ch06_sql_window.solutions",
    "ch07_udf_catalyst.examples", "ch07_udf_catalyst.solutions",
    "ch08_data_sources.examples", "ch08_data_sources.solutions",
    "ch09_performance.examples", "ch09_performance.solutions",
    "ch10_streaming.examples", "ch10_streaming.solutions",
    "ch11_mllib.examples", "ch11_mllib.solutions",
    "ch13_capstone.pipeline",
]

if __name__ == "__main__":
    filters = sys.argv[1:]
    failed = []
    for module in MODULES:
        if filters and not any(f in module for f in filters):
            continue
        print(f"\n########## {module} ##########", flush=True)
        if subprocess.run([sys.executable, "-m", module]).returncode != 0:
            failed.append(module)
    print("\nFAILED:", failed or "none")
    sys.exit(1 if failed else 0)
