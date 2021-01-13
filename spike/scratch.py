import zipfile
from pathlib import Path

from project_settings import get_root_path

root_path = get_root_path()
if Path(f"{root_path}/instantclient_19_8").is_dir():
    print("am here")
    pass
else:
    with zipfile.ZipFile(f"{root_path}/instantclient_19_8.zip", "r") as zip_ref:
        zip_ref.extractall(f"{root_path}/instantclient_19_8")
