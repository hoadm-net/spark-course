import sys
from pathlib import Path

# Make the repository root importable when running `pytest ch12_deployment`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
