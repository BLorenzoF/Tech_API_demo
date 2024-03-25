
import os
import sys
# Add the src directory to the path
os.environ["JUPYTER_PLATFORM_DIRS"] = "1"
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "danelfin_demo"))
)


