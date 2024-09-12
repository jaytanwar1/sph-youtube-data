import pandas as pd
import boto3
import json
import datetime
from tqdm import tqdm




class SPHDataTransformIngest:

    def __init__(self, ) -> None:
        # Creating an s3 client
        s3_client = boto3.client('s3')
        self.s3_client = s3_client
        self.username = "JAY-SPH-ASSIGNMENT"
        self.ETL_INSERT_BY = self.username
        self.vid_stat_df = pd.DataFrame(columns=['Title', "LikeCount", "CommentCount", "ViewCount", "publishedAt", "channelId", "videoId", "ETL_INSERT_DATE","ETL_INSERT_BY"])

    def run(self, Clean_Bucket_name, start_date, end_date):

        print("Starting Script 3")
        date_range = pd.date_range(start_date, end_date, freq='H')
        file_count = 0
        for itr_date in tqdm(date_range):

            itr_start_year = itr_date.year
            itr_date_month = itr_date.month
            itr_date_day = itr_date.day
            itr_date_hour = itr_date.hour

            file_name = f"{str(itr_start_year)}/{str(itr_date_month)}/{str(itr_date_day)}/{str(itr_date_hour)}/clean_data_from_{str(itr_start_year)}-{str(itr_date_month)}-{str(itr_date_day)}-{str(itr_date_hour)}"
            #print(file_name)

            try:

                res = self.s3_client.get_object(Bucket=Clean_Bucket_name, Key=file_name)
                data = res['Body'].read()
                vid_stats_final = self.clean_data(data=data)
                #pd.set_option('display.max_rows', None)  # Set to None to display all rows
                #pd.set_option('display.max_columns', None)  # Set to None to display all columns
                #print(t_series)

            except Exception as e:
                error = str(e)
                file_count = file_count + 1
                #print(f"error occured : {error}")
                continue

        print(vid_stats_final)
        json_string = json.dumps(vid_stats_final.to_dict(orient='records'))
        json_buffer =bytes(json_string, 'utf-8')
        file_name = "new_file"+str(end_date)
        self.s3_client.put_object(Bucket="sph-tgt-data", Key=file_name, Body=json_buffer)

        metadata = {"last_update_date":str(end_date)}
        metadata_json = json.dumps(metadata)
        self.s3_client.put_object(Bucket="sph-bookmark", Key="Bookmark", Body=metadata_json)
        print("Ending Script 3")



    def clean_data(self, data):

        data = json.loads(data)
        ViewCount = data['viewCount']
        LikeCount = data['likeCount']
        CommentCount = data['commentCount']
        publishdate = data['publishedAt']
        Title = str(data['Title'])
        ChannelId = str(data['channelId'])
        VideoId = str(data['videoId'])
        ETL_INSERT_DATE = datetime.datetime.now()
        ETL_INSERT_BY = self.ETL_INSERT_BY

        new_row = {'Title': Title, 'LikeCount': LikeCount, 'CommentCount': CommentCount, "ViewCount": ViewCount,
                   "publishedAt": publishdate, "channelId":ChannelId, "videoId":VideoId, "ETL_INSERT_DATE": str(ETL_INSERT_DATE),
                   "ETL_INSERT_BY": str(ETL_INSERT_BY)}

        self.vid_stat_df.loc[len(self.vid_stat_df)] = new_row

        return self.vid_stat_df


if __name__ == "__main__":
    Clean_Bucket_name ="sph-clean-data"
    start_date = datetime.datetime(2024,9, 5 , 6)
    end_date = datetime.datetime(2024,9,11,8)
    # start_date = datetime.datetime(2023, 10, 1 , 11)
    # end_date = datetime.datetime(2023,10,8,11)

    SPHDataTransformIngestObj = SPHDataTransformIngest()
    SPHDataTransformIngestObj.run(Clean_Bucket_name , start_date , end_date)