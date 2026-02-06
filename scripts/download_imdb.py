from huggingface_hub import hf_hub_download
from pathlib import Path
import shutil

dest = Path("data/source/IMDB_Dataset.csv")
dest.parent.mkdir(parents=True, exist_ok=True)

path = hf_hub_download(
    repo_id="scikit-learn/imdb",
    filename="IMDB Dataset.csv",
    repo_type="dataset",
)

shutil.copy(path, dest)
print("Saved IMDb dataset to data/source/IMDB_Dataset.csv")
