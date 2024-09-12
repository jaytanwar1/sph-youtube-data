import boto3
import json


def Increamental_Load():
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
    api_key = "AIzaSyCdaIcTZH726agV2K9WFQi-VgcWjn7__Qo"

    """
    This is @straitstimesonline channel id
    """
    channel_id = 'UC4p_I9eiRewn2KoU-nawrDg'

    # Creating an instance of our class FullLoadDataToS3
    FullLoadDataToS3_obj = FullLoadDataToS3(api_key, channel_id)
    Bucket_name = "my-bucket-2521"
    now = datetime.datetime.now()
    # input_format="%Y-%m-%dT%H:%M:%SZ"
    # output_format="%Y-%m-%d %H:%M:%S.%f"
    # date_object = datetime.datetime.strptime(a_week, input_format)
    date_object = a_week
    a_week = datetime.datetime.strptime(date_object, '%Y-%m-%d %H:%M:%S.%f')

    # Format the datetime object to the desired format
    # a_week = date_object.strftime(output_format)
    # a_week = datetime.datetime.strptime(a_week, output_format)

    # a_week = datetime.datetime.strptime(a_week, output_format)

    raw_file_name = FullLoadDataToS3_obj.run(End_Date=a_week, Bucket_Name=Bucket_name)
    print(raw_file_name[1])
    if raw_file_name[1] != 0:
        raw_bucket_name = "sph-raw-data"
        clean_bucket_name = "sph-clean-data"
        # raw_file_name = "put_data_from2023-9-29-7_to_2023-10-6-7"

        RawToProcess = RawToProcess(raw_bucket_name, clean_bucket_name)
        RawToProcess.run(raw_file_name=raw_file_name[0])

        Clean_Bucket_name = "sph-clean-data"
        start_date = a_week
        end_date = now

        # SnowFlakeDataIngestObj = SnowFlakeDataIngest()
        # SnowFlakeDataIngestObj.run(Clean_Bucket_name, start_date, end_date)
        # Call the Class SPHDataTransformIngest
        SPHDataTransformIngestObj = SPHDataTransformIngest()
        SPHDataTransformIngestObj.run(Clean_Bucket_name, start_date, end_date)
    else:
        print("Data execution stoped due no videos")


if __name__ == "__main__":
    Increamental_Load()