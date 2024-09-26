import boto3
import json


def Increamental_Load():
    try:
        s3_client = boto3.client('s3')
        # self.s3_client = s3_client
        response = s3_client.get_object(Bucket="sph-bookmark", Key="Bookmark")
        # Deserialize the JSON object to a dictionary
        metadata = json.loads(response['Body'].read().decode('utf-8'))
        print(metadata)
        a_week = metadata['last_update_date']
        print(a_week)
        from LoadSPHdata import FullLoadDataToS3
        from PreProcessSPHdata import RawToProcess
        from TransformSPHdata import SPHDataTransformIngest
        import datetime
        api_key = "" # add api key

        channel_ids = [
            'UC4p_I9eiRewn2KoU-nawrDg',  # Strait Times
            'UC0GP1HDhGZTLih7B89z_cTg',  # Business Times
            'UCrbQxu0YkoVWu2dw5b1MzNg',  # ZaoBao
            'UCs0xZ60FSNxFxHPVFFsXNTA',  # Tamil Marusu
            'UC_WgSFSkn7112rmJQcHSUIQ'  # Berita Harian
        ]

        # Creating an instance of our class FullLoadDataToS3
        #FullLoadDataToS3_obj = FullLoadDataToS3(api_key, channel_id)
        Bucket_name = "sph-raw-data" #"my-bucket-2521"
        Stats_Bucket_Name = "sph-stats-data"
        now = datetime.datetime.now()
        date_object = a_week
        a_week = datetime.datetime.strptime(date_object, '%Y-%m-%d %H:%M:%S.%f')

        # Format the datetime object to the desired format
        # a_week = date_object.strftime(output_format)
        # a_week = datetime.datetime.strptime(a_week, output_format)
        for channel_id in channel_ids:
            print(f"Processing data for Channel ID: {channel_id}")
            FullLoadDataToS3_obj = FullLoadDataToS3(api_key, channel_id)
            #FullLoadDataToS3_obj.get_channel_statistics(Stats_Bucket_Name)
            raw_file_name = FullLoadDataToS3_obj.run(End_Date=a_week, Bucket_Name=Bucket_name)
            print(raw_file_name[1])
            if raw_file_name[1] != 0:
                raw_bucket_name = "sph-raw-data"
                clean_bucket_name = "sph-clean-data"
                # raw_file_name = "put_data_from2023-9-29-7_to_2023-10-6-7"

                raw_to_process_obj = RawToProcess(raw_bucket_name, clean_bucket_name)
                raw_to_process_obj.run(raw_file_name=raw_file_name[0])

                Clean_Bucket_name = "sph-clean-data"
                start_date = a_week
                end_date = now

                SPHDataTransformIngestObj = SPHDataTransformIngest()
                SPHDataTransformIngestObj.run(Clean_Bucket_name, start_date, end_date)
                print(f"Data uploaded successfully for Channel ID: {channel_id}")
            else:
                print(f"No new videos for Channel ID: {channel_id}, skipping.")

    except Exception as e:
        print(f"Error in Incremental Load: {str(e)}")

if __name__ == "__main__":
    Increamental_Load()