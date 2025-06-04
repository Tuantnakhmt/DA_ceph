import sys
sys.path.append('/app/')

from Flow import Folder
from VAR_GLOBAL_CONFIG import *
print(f"END_DAY_UPDATE: {END_DAY_UPDATE}")
print("aaa")
create = Folder.FolderUpdate(date=END_DAY_UPDATE)
print("FolderUpdate instance created.")
create.Run_Create_Folder()
print("Folder creation completed successfully.")


