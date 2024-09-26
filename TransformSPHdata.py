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
                #print(vid_stats_final)

                for channel_id, group in vid_stats_final.groupby('channelId'):
                    # Convert the group of data to JSON with each record on a new line
                    json_records = group.to_dict(orient='records')
                    formatted_date = end_date.strftime('%d%m%y%H%M')
                    partitioned_file_name = f"channelid={channel_id}/sphYTtgtdata_{formatted_date}.json"

                    # Convert the data to JSON and upload it directly to S3 without saving to the local /tmp/ directory
                    json_string = "\n".join([json.dumps(record) for record in json_records])

                    # Upload the JSON string directly to S3 partitioned by channel_id
                    self.s3_client.put_object(Bucket="sph-tgt-data", Key=partitioned_file_name, Body=json_string)

            except Exception as e:
                error = str(e)
                file_count = file_count + 1
                #print(f"error occured : {error}")
                continue

        print(vid_stats_final)
        # json_records = vid_stats_final.to_dict(orient='records')
        # formatted_date = end_date.strftime('%d%m%y%H%M')
        # file_name = f"sphYTtgtdata_{formatted_date}.json"
        #
        # json_string = "\n".join([json.dumps(record) for record in json_records])
        #
        # self.s3_client.put_object(Bucket="sph-tgt-data", Key=file_name, Body=json_string)
        #
        metadata = {"last_update_date":str(end_date)}
        print(metadata)
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
    start_date = datetime.datetime(2024,9, 20 , 2)
    end_date = datetime.datetime(2024,9,21,12)
    # start_date = datetime.datetime(2023, 10, 1 , 11)
    # end_date = datetime.datetime(2023,10,8,11)

    SPHDataTransformIngestObj = SPHDataTransformIngest()
    SPHDataTransformIngestObj.run(Clean_Bucket_name , start_date , end_date)