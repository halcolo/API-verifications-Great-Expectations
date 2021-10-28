import sys
import os

def get_project_path():
    """return project path

    Returns:
        str: project path
    """
    project_path = os.path.dirname(os.path.abspath("top_level_file.txt"))
    return project_path