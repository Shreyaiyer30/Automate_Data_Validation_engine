import sys
import os
import traceback

sys.path.append(os.getcwd())

try:
    from ui.pages.upload import render
    print("Successfully imported render from ui.pages.upload")
except Exception:
    traceback.print_exc()
