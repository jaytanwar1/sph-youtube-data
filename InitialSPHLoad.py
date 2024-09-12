import datetime

def full_load_fun() -> str:
    try:
        #Importing modules
        from LoadSPHdata import FullLoadDataToS3
        from PreProcessSPHdata import RawToProcess
        from TransformSPHdata import SPHDataTransformIngest

        print("Full Load function started .... /n  Here we take data of past 7 days")

        """ Parameter required for the classes """

        # Youtube API Key
        Api_Key = "AIzaSyCdaIcTZH726agV2K9WFQi-VgcWjn7__Qo"
        """
        This is @straitstimesonline channel id
        """

        Channel_Id = 'UC4p_I9eiRewn2KoU-nawrDg'

        # Create a date that is 7 days before now
        Today_Date = datetime.datetime.now()
        Seven_Day_Delta_Time = datetime.timedelta(days=30)
        # Subtract Seven day from the current datetime
        Week_Past_Date = Today_Date - Seven_Day_Delta_Time

        # Bucket_name of aws S3 that we have created manually
        Raw_Data_Loading_Bucket_Name = "sph-raw-data"
        Clean_Bucket_Name = "sph-clean-data"

        Extracting_Date_Range = f"Extracting Data from {Seven_Day_Delta_Time} to {Today_Date}"
        print(Extracting_Date_Range)

        """Calling Classes and its Methods"""
        # Creating an instance of our class FullLoadDataToS3
        FullLoadDataToS3_obj = FullLoadDataToS3(Api_Key, Channel_Id)

        # Call the `FullLoadDataToS3_obj.run()` function with the date 7 days ago as the argument and bucket_name
        Raw_File_Name = FullLoadDataToS3_obj.run(End_Date=Week_Past_Date, Bucket_Name=Raw_Data_Loading_Bucket_Name)

        if Raw_File_Name[1] != 0:

            # Call the class RawToProcess
            RawToProcess = RawToProcess(Raw_Data_Loading_Bucket_Name, Clean_Bucket_Name)
            RawToProcess.run(raw_file_name=Raw_File_Name[0])

            start_date = Week_Past_Date
            end_date = Today_Date

            # Call the Class SPHDataTransformIngest
            SPHDataTransformIngestObj = SPHDataTransformIngest()
            SPHDataTransformIngestObj.run(Clean_Bucket_Name, start_date, end_date)

            return "Data uploadedSucessfully"

        else:
            print("Data execution stoped due no videos")

    except Exception as e:

        return "Error in Fullload :- ", str(e)

if __name__ == "__main__":
    full_load_fun()