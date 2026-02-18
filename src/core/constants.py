PIPELINE_STEPS = [
    ("📁", "Upload"),
    ("📊", "Overview"),
    ("✅", "Validate"),
    ("🧹", "Clean"),
    ("📈", "Visualize"),
    ("📦", "Export"),
]

PAGES = [
    ("🏠", "Home",        None),
    ("📁", "Upload",      None),
    ("📊", "Overview",    "raw_df"),
    ("✅", "Validate",    "raw_df"),
    ("🧹", "Clean",       "raw_df"),
    ("📈", "Visualize",   "clean_df"),
    ("📦", "Export",      "run_result"),
]
