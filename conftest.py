# conftest.py

import os
import sys

# Compute the absolute path to the `dags/` folder
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DAGS_FOLDER   = os.path.join(PROJECT_ROOT, "dags")

# Prepend `dags/` so that `import common.tasks` works
if DAGS_FOLDER not in sys.path:
    sys.path.insert(0, DAGS_FOLDER)
