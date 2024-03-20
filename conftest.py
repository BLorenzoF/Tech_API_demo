import sys
import os

# Add the src directory to the path
os.environ["JUPYTER_PLATFORM_DIRS"] = "1"
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "{{ cookiecutter.project_name }}"))
)
