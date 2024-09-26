import boto3
import json
import datetime


class RawToProcess:

    def __init__(self,raw_bucket_name,clean_bucket_name) -> None:
        """
        Initializes the class with the names of the raw and clean buckets.

        Args:
            raw_bucket_name: The name of the raw bucket.
            clean_bucket_name: The name of the clean bucket.
        """

        self.raw_bucket_name = raw_bucket_name
        self.clean_bucket_name = clean_bucket_name
        #Creating an s3 client
        s3_client = boto3.client('s3')
        self.s3_client = s3_client

    def fetch_data_from_raw_bucket(self,raw_file_name) ->json:
        """
        fetches the data from the raw bucket.
        Args :-
        raw_file_name :- The name of the file in raw s3bucket

        """

        #Read json data
        res = self.s3_client.get_object(Bucket = self.raw_bucket_name,Key = raw_file_name)
        return res['Body'].read()

    def pre_process(self, data) -> list:
        """

        Pre-processes the raw data.

        Args:
            data: The raw data as a bytes object.

        Returns:
            A list of two elements:
                1. A list of pre-processed data items.
                2. A dictionary containing the metadata of the data.

        """

        # Initialize a list to store the pre-processed data.
        new_all_json_data = []

        # Load the JSON data.
        json_data = json.loads(data)

        # Get the metadata from the JSON data.

        # Iterate over the JSON data and pre-process each record.
        for itr in json_data:
            # Initialize a dictionary to store the pre-processed record.
            itr_pre_processed_json = {}

            # Load the JSON data for the current record.
            itr_json_data = json.loads(itr)
            #print(itr_json_data)

            # Get the statistics for the current record.
            itr_pre_processed_json = itr_json_data['items'][0]['statistics']

            # Add the title of the current record to the pre-processed record.
            itr_pre_processed_json['Title'] = itr_json_data['items'][0]['snippet']['title']

            # Published date
            Published_date = itr_json_data['items'][0]['snippet']['publishedAt']
            Channel_id = itr_json_data['items'][0]['snippet']['channelId']
            Video_id = itr_json_data['items'][0]['id']
            #print(f"channel id : {Channel_id} and video id : {Video_id}")
            itr_pre_processed_json['publishedAt'] = Published_date
            itr_pre_processed_json['channelId'] = Channel_id
            itr_pre_processed_json['videoId'] = Video_id
            Published_date = datetime.datetime.strptime(str(Published_date), "%Y-%m-%dT%H:%M:%SZ")

            print(f"channel id : {Channel_id} , video_id {Video_id} ,  publish date : {Published_date}")

            self.distribute_data(itr_pre_processed_json, Published_date)

            # Add the pre-processed record to the list of pre-processed data.
            new_all_json_data.append(itr_pre_processed_json)

        # Return the pre-processed data with the meta data.
        return [new_all_json_data]

    def distribute_data(self, data, date):
        """
        Distributes the pre-processed data to the clean bucket.

        Args:
            data: A list of pre-processed data items.
            meta_data: A dictionary containing the metadata of the data.

        Returns:
            A string indicating whether the data was distributed successfully.
        """
        try:

            # Get the partition keys from the metadata.
            year_partition = date.year
            month_partition = date.month
            day_partition = date.day
            hour_partition = date.hour
            date_ = f"{year_partition}-{month_partition}-{day_partition}-{hour_partition}"

            # Get the start and end dates from the metadata.
            s3_object_key = f"clean_data_from_{date_}"
            s3_file_name = f"{year_partition}/{month_partition}/{day_partition}/{hour_partition}/{s3_object_key}"

            # Convert the data to JSON and encode it in UTF-8
            json_data = json.dumps(data)
            json_object = json.loads(json_data)

            # Construct the S3 object key.
            #print("dump to clean")
            data = json.dumps(json_object).encode('UTF-8')
            self.s3_client.put_object(Body=data, Bucket=self.clean_bucket_name, Key=s3_file_name)

            return "Sucess"
        except Exception as e:

            print(f"Error found in distribute_data:- {str(e)}")
            return "Failed"


    def run(self, raw_file_name) -> None:
        """
        Processes the raw data file and stores the cleaned data in the clean bucket.

        Args:
            raw_file_name: The name of the raw data file.
        """

        try:
            print("Starting Script 2 ")

            # Fetch the data from the raw bucket.
            Response = self.fetch_data_from_raw_bucket(raw_file_name=raw_file_name)

            # Pre-process the data.
            pre_processed_data = self.pre_process(Response)

            # Distribute the data in the clean bucket.
            # results = self.distribute_data(pre_processed_data[0] , pre_processed_data[1])

            # Print the results.
            # print(f"Results:- {results}")
            print("Ending Script 2")

        except Exception as e:
            print("Failed")
            print("Error found in RawToProcess:- ", str(e))


if __name__ == "__main__":
    raw_bucket_name = "sph-raw-data"
    clean_bucket_name = "sph-clean-data"
    raw_file_name = "put_data_from2024-9-19-15_to_2024-9-22-15"

    RawToProcess = RawToProcess(raw_bucket_name , clean_bucket_name)
    RawToProcess.run(raw_file_name = raw_file_name)