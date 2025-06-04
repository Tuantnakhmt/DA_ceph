# from datetime import datetime
# import os
# from typing import Type
# from Flow.PATH_env import PATH_ENV


# class FolderData(PATH_ENV):
#     """
#     Base class for managing folder creation.
#     """

#     def __init__(self, Type_, date):
#         """
#         Initialize folder data handler.

#         Parameters:
#         - Type_ : str : Type of Data (e.g., "Ingestion", "Raw_VIS", "WH").
#         - date : str : Date for folder management in YYYY-MM-DD format.
#         """
#         if len(date) == 0:
#             super().__init__(Type_, RealDay=True)
#         else:
#             super().__init__(Type_, date, RealDay=False)

#     def createFolder(self, path):
#         """
#         Create a folder if it doesn't exist.

#         Parameters:
#         - path : str : Path to the folder.

#         Returns:
#         - str : Created path.
#         """
#         if not os.path.exists(path):
#             os.makedirs(path)
#         return path

#     def getDateUpdate(self, DAY):
#         """
#         Get the most recent update date.

#         Parameters:
#         - DAY : str : Target date in YYYY-MM-DD format.

#         Returns:
#         - str : Closest available date.
#         """
#         list_date = [d for d in os.listdir(self.PATH_MAIN) if len(d) == 10]
#         list_date.sort(reverse=True)
#         for i in list_date:
#             if i <= DAY:
#                 return i
#         return DAY

#     def getListPath(self):
#         """
#         Get a list of sub-paths in the main directory.

#         Returns:
#         - list : List of sub-directory names.
#         """
#         return os.listdir(self.PATH_MAIN)


# class FolderCrawl(FolderData):
#     """
#     Create folders for ingestion/crawling data.
#     """

#     def __init__(self, date=""):
#         super().__init__("Ingestion", date)

#     def folderClose(self):
#         """Create folders for Close data."""
#         path = self.PATH_CLOSE
#         self.createFolder(path)
#         for obj in self.CloseObject:
#             self.createFolder(self.joinPath(path, obj))

#     def folderDividend(self):
#         """Create folders for Dividend data."""
#         path = self.PATH_DIVIDEND
#         self.createFolder(path)
#         for obj in self.DividendObject:
#             self.createFolder(self.joinPath(path, obj))

#     def folderFinancial(self):
#         """Create folders for Financial data."""
#         path = self.PATH_FINANCIAL
#         self.createFolder(path)
#         for obj in self.FinancialObject:
#             for t_time in self.Type_Time:
#                 for p_obj in self.FinancialPartObject:
#                     self.createFolder(self.joinPath(path, obj, t_time, p_obj))

#     def folderVolume(self):
#         """Create folders for Volume data."""
#         path = self.PATH_VOLUME
#         self.createFolder(path)
#         for obj in self.VolumeObject:
#             for p_obj in self.VolumePartObject:
#                 self.createFolder(self.joinPath(path, obj, p_obj))

#     def runCreateFolder(self):
#         """Run the folder creation process."""
#         self.folderClose()
#         self.folderDividend()
#         self.folderFinancial()
#         self.folderVolume()


# class FolderUpdate(FolderData):
#     """
#     Create folders for updating raw data.
#     """

#     def __init__(self, date):
#         super().__init__("Raw_VIS", date=date)

#     def folderClose(self):
#         """Create folders for Close data updates."""
#         path = self.PATH_CLOSE
#         self.createFolder(self.joinPath(path, "CafeF", "F0"))
#         self.createFolder(self.joinPath(path, "CafeF", "F1"))

#     def folderDividend(self):
#         """Create folders for Dividend data updates."""
#         path = self.PATH_DIVIDEND
#         self.createFolder(path)
#         for obj in self.DividendObject:
#             for P_F in self.Phase[:3]:
#                 self.createFolder(self.joinPath(path, obj, P_F))

#     def folderFinancial(self):
#         """Create folders for Financial data updates."""
#         path = self.PATH_FINANCIAL
#         self.createFolder(path)
#         for obj in self.FinancialObject:
#             for P_F in self.Phase:
#                 for t_time in self.Type_Time:
#                     for p_o in self.FinancialPartObject:
#                         self.createFolder(self.joinPath(path, obj, P_F, t_time, p_o))

#     def folderVolume(self):
#         """Create folders for Volume data updates."""
#         path = self.PATH_VOLUME
#         for obj in self.VolumeObject:
#             for PHASE in self.Phase[:2]:
#                 for p_obj in self.VolumePartObject:
#                     self.createFolder(self.joinPath(path, obj, PHASE, p_obj))

#     def folderCompare(self):
#         """Create folders for Compare data updates."""
#         path = self.PATH_COMPARE
#         for time in self.Type_Time:
#             self.createFolder(self.joinPath(path, "Financial", time))
#         self.createFolder(self.joinPath(path, "Dividend"))
#         self.createFolder(self.joinPath(path, "Error"))

#     def runCreateFolder(self):
#         """Run the folder creation process for updates."""
#         self.folderClose()
#         self.folderDividend()
#         self.folderFinancial()
#         self.folderVolume()
#         self.folderCompare()


# class FolderWH(FolderData):
#     """
#     Create folders for Warehousing.
#     """

#     def __init__(self, date=""):
#         super().__init__("WH", date)


# # # Example usage
# # if __name__ == "__main__":
# #     # Create folders for ingestion
# #     ingestion_folder = FolderCrawl(date="")
# #     ingestion_folder.runCreateFolder()

# #     # Create folders for updating raw data
# #     update_folder = FolderUpdate(date="2024-11-18")
# #     update_folder.runCreateFolder()

# #     print("Folders created successfully!")
import os
from Flow import PATH_env
import json


class FolderData(PATH_env.PATH_ENV):
    '''
    Create Folder for Data'''
    def __init__(self, Type_, date):
        '''
        Type_: Type of Data \n
        date: date of Data'''
        if len(date) == 0:
            super().__init__(Type_, RealDay=True)
        else:
            super().__init__(Type_, date, RealDay=False)

    # def createFolder(self, path):
    #     '''
    #     Create Folder \n
    #     Input: path \n
    #     Output: path'''
    #     isExist = os.path.exists(path)
    #     if not isExist:
    #         os.makedirs(path)
    #     return path
    def createFolder(self, path):
        print(f"Attempting to create folder: {path}")
        try:
            os.makedirs(path, exist_ok=True)
            print(f"Folder created (or already exists): {path}")
        except Exception as e:
            print(f"Error creating folder {path}: {e}")

    def GetDateUpdate(self, DAY):
        '''
        Get Date Update \n
        Input: DAY: date \n
        Output: date'''
        list_date = os.listdir(self.PATH_MAIN)
        arr = []
        for day in list_date:
            if len(day) == 10:
                arr.append(day)
        arr.sort(reverse=True)
        for i in arr:
            if i <= DAY:
                return i
        return DAY

    def getListPath(self):
        '''
        Get List Path \n
        Output: list path'''
        return os.listdir(self.PATH_MAIN)


class FolderCrawl(FolderData):
    def __init__(self, date=""):
        '''
        Create Folder for Crawl Data \n
        Input: date'''

        super().__init__("Ingestion", date)

    def folderClose(self):
        '''
        Create Folder for Close Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_CLOSE
        self.createFolder(path)
        for obj in self.CloseObject:
            self.createFolder(self.joinPath(path, obj))

    def folderDividend(self):
        '''
        Create Folder for Dividend Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_DIVIDEND
        self.createFolder(path)
        for obj in self.DividendObject:
            if obj == "VietStock":
                pass
                # for p_obj in self.DividendPartObject:
                #     self.createFolder(self.joinPath(path,obj,p_obj))
            else:
                self.createFolder(self.joinPath(path, obj))

    def folderFinancial(self):
        '''
        Create Folder for Financial Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_FINANCIAL
        self.createFolder(path)
        for obj in self.FinancialObject:
            for t_time in self.Type_Time:
                for p_obj in self.FinancialPartObject:
                    self.createFolder(self.joinPath(path, obj, t_time, p_obj))

    def folderVolume(self):
        '''
        Create Folder for Volume Data \n
        Input: None \n
        Output: None'''

        path = self.PATH_VOLUME
        self.createFolder(path)
        for obj in self.VolumeObject:
            for p_obj in self.VolumePartObject:
                self.createFolder(self.joinPath(path, obj, p_obj))

    def Run_Create_Folder(self):
        '''
        Run Create Folder \n
        '''
        self.folderClose()
        self.folderDividend()
        self.folderFinancial()
        self.folderVolume()


class FolderUpdate(FolderData):
    '''
    Create Folder for Update Data'''
    def __init__(self, date):
        '''
        NeedFolderUpdate: list folder need update \n'''
        super().__init__("Ingestion", date=date)
        # self.PATH_BASE = "/data"  # Set the base path to /data
        # print(f"Base path set to: {self.PATH_BASE}")
        self.NeedFolderUpdate = []

    def folderClose(self):
        '''
        Create Folder for Close Data \n'''
        path = self.PATH_CLOSE
        # for obj in self.CloseObject:
        #     for PHASE in self.Phase[]:
        self.createFolder(self.joinPath(path, "CafeF", "F0"))
        self.createFolder(self.joinPath(path, "CafeF", "F1"))

    def folderDividend(self):
        '''
        Create Folder for Dividend Data \n
        Input: None \n
        Output: None    '''
        path = self.PATH_DIVIDEND
        self.createFolder(path)
        for obj in self.DividendObject:
            for P_F in self.Phase[:3]:
                if obj == "VietStock" and P_F == "F0":
                    self.createFolder(self.joinPath(path, obj, self.Temp))
                    for p_obj in self.DividendPartObject:
                        self.createFolder(self.joinPath(path, obj, P_F, p_obj))
                else:
                    self.createFolder(self.joinPath(path, obj, P_F))

    def folderFinancial(self):
        '''
        Create Folder for Financial Data \n
        Input: None \n
        Output: None
        '''
        path = self.PATH_FINANCIAL
        self.createFolder(path)
        for obj in self.FinancialObject:
            for P_F in self.Phase:
                for t_time in self.Type_Time:
                    if P_F == "F0":
                        if obj == "VietStock":
                            self.createFolder(self.joinPath(path, obj, self.Temp))
                        for p_o in self.FinancialPartObject:
                            self.createFolder(
                                self.joinPath(path, obj, P_F, t_time, p_o)
                            )
                    else:
                        for p_o in self.FinancialPartObject:
                            self.createFolder(self.joinPath(path, obj, P_F, t_time))

    def folderVolume(self):
        '''
        Create Folder for Volume Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_VOLUME
        for obj in self.VolumeObject:
            for PHASE in self.Phase[:2]:
                for p_obj in self.VolumePartObject:
                    self.createFolder(self.joinPath(path, obj, PHASE, p_obj))

    def folderCompare(self):
        '''
        Create Folder for Compare Data \n
        Input: None \n
        Output: None'''
        path = self.PATH_COMPARE
        for time in self.Type_Time:
            self.createFolder(self.joinPath(path, "Financial", time))
        self.createFolder(self.joinPath(path, "Dividend"))
        self.createFolder(self.joinPath(path, "Error"))

    def Run_Create_Folder(self):
        print("Starting folder creation process...")
        self.folderClose()
        print("Finished creating Close folders.")
        self.folderDividend()
        print("Finished creating Dividend folders.")
        self.folderFinancial()
        print("Finished creating Financial folders.")
        self.folderVolume()
        print("Finished creating Volume folders.")
        self.folderCompare()
        print("Finished creating Compare folders.")
        print("Folder creation process completed.")


class FolderWH(FolderData):
    '''
    Create Folder for WareHouse Data'''
    def __init__(self, date=""):
        super().__init__("WH", date)
