
import os
import sys
import shutil
# Add the src directory to the path
os.environ["JUPYTER_PLATFORM_DIRS"] = "1"
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "danelfin_demo/test"))
)
def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    os.remove(os.getcwd() + '/db.json')
    shutil.rmtree('dump')


