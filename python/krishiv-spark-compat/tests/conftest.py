import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path = [p for p in sys.path if "krishiv-python" not in p]
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "krishiv/compat/spark/_proto"))
