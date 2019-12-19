from pathlib import Path

data_dir = Path(__file__).parents[1].joinpath("data")
data_files = {f.name: f for f in data_dir.iterdir()}
