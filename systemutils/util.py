from pathlib import Path
import os
import sys


def get_project_root() -> Path:
    return Path(__file__).parent.parent
