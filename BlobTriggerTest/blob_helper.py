from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import date_helpers
import datetime
from tabulate import tabulate
import pandas as pd
from io import StringIO


class BlobHelper:
    def __init__(self, connection_string, container_name, path_to_dir):
        """
        Description/Functionality:
            This code defines a constructor for a class that initializes the 
            instance variables connection_string, container_name, and 
            path_to_dir with the values passed as arguments. It also 
            initializes the instance variable container_client to 
            None and then calls the connectToBlob() method to connect 
            to the Azure Blob Storage container specified in the instance variables.

        Args:

            connection_string: a string that contains the connection string for the Azure Blob Storage account
            container_name: a string that specifies the name of the container in the Azure Blob Storage account
            path_to_dir: a string that specifies the path to the local directory where the blob files will be downloaded.
        
        Returns:
            This code does not return any value, but it initializes the instance variables and calls the connectToBlob() method to connect to the Azure Blob Storage container.
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.path_to_dir = path_to_dir
        self.container_client = None
        self.month = None
        self.connectToBlob()
    
    
    def connectToBlob(self):
        """
        Description/Functionality:
        This code defines a method that connects to an Azure Blob Storage container
        using the connection string and container name stored in the instance variables 
        of a class. It does this by using the from_connection_string method of the 
        BlobServiceClient class from the azure.storage.blob module, which takes the 
        connection string as an argument to create a new BlobServiceClient object. 
        Then it retrieves the container client object from the BlobServiceClient object 
        using the get_container_client method and assigns it to the instance variable self.container_client.

        Args:
        This code assumes that the following variables are already defined:

        self: an instance of a class that has connection_string and container_name instance variables.
        
        Returns:
        This code does not return any value. Instead, it assigns a value to the instance variable self.container_client, which is used in other methods of the class to access the container.
        """
        try:
            connection_string = self.connection_string
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            print('Connected to Blob Storage!\n')

        except Exception as ex:
            print('Exception:')
            print(ex)
            
        container_name = self.container_name
        self.container_client = blob_service_client.get_container_client(container_name)
    
    def get_directory_from_date (self, date_str = None) -> str:
        '''
        Gets the directory name from the given date.
        
        The directory is an interval of 1 month.
        
        Checks if the given date is None. If it is None, it sets the date to the current date.
        
        If the given date is not None, uses the **date_helpers** module to get the start and end dates of the interval.
        
        Performs a .join() on the start and end dates to get the directory name.
        
        returns the directory name as a string.
        
        '''
        
        if date_str is None:
            tmpDate = datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            tmpDate = date_str
        startDate = date_helpers.Date_Interval_Start_Str(
            date_str=tmpDate, interval='month'
        )
        endDate = date_helpers.Date_Interval_End_Str(
            date_str=tmpDate, interval='month'
        )
        finalDates = [startDate, endDate]
        finalDates = [date_helpers.date_dt(date_str=x).strftime('%Y%m%d') for x in finalDates]
        finalStr = '_'.join(finalDates)
        
        return finalStr

    def get_dir_from_dateNID(self, date, ID) -> dict:
        '''
        This function retrieves file names from the blob storage based on the given date and ID.
        
        It first gets a list of blobs from the container_client object that have names starting with 'new/dailyEAUsage/'.
        
        It then normalizes the given date and replaces any underscores with dashes.
        
        A new dictionary desired_files is created to store the file names with the creation date as the key.
        
        The function iterates over the list of blobs and checks if the content_disposition property of the blob is not None. If it is not None, the function splits the content_disposition property to extract the file name.
        
        The function then checks if the given ID is in the file name and the normalized date is in the blob name. If the conditions are met, the function gets the creation time of the blob in the '%Y-%m-%d' format and stores the file name in the desired_files dictionary with the creation time as the key.
        
        Finally, the function returns the desired_files dictionary containing the file names with their creation date as the key.
        '''
        self.month = self.get_month_from_date(date)
        print(f'Getting desired blobs from {self.container_name} on month: {self.month}\n')
        blob_list = self.container_client.list_blobs(name_starts_with=self.path_to_dir)
        normalizedDate = self.get_directory_from_date(date).replace('_', '-')
        desired_files = {}
        enrolmentID_count = 0
        for blob in blob_list:
            month = blob.creation_time.month
            year = blob.creation_time.year
            if blob.content_settings.content_disposition is not None:
                reshaped = blob.content_settings.content_disposition.split(';')[1].split('=')[1]
                creationT_naive = blob.creation_time.replace(tzinfo=None)
                # If the blob is in the daily container, it checks if the creation time is after the 19th of the month
                if 'daily' in self.container_name:
                    if str(ID) in reshaped and normalizedDate in blob.name and creationT_naive >= datetime.datetime(year, month, 19, 0, 0, 0):
                        enrolmentID_count += 1
                        date_name = blob.creation_time.strftime('%Y-%m-%d')
                        desired_files[date_name] = blob.name
                # If the blob is in the monthly container, wont check the creation time
                elif 'monthly' in self.container_name:
                    if str(ID) in reshaped and normalizedDate in blob.name:
                        enrolmentID_count += 1
                        date_name = blob.creation_time.strftime('%Y-%m-%d')
                        desired_files[date_name] = blob.name
        return desired_files
    
    def tabulate_desired_blob(self, blobsDict:dict,headers=['Blob Name', 'Creation Date'])-> None:
        table = [ [name, date] for date, name in blobsDict.items()]
        print('Tabulating desired blobs...\n')
        print(tabulate(table, headers=headers, tablefmt='psql'))
    
    def read_blob(self, blob_name) -> pd.DataFrame:
        """
        Description/Functionality:
            This code defines a function that reads a blob file from an 
            Azure Blob Storage container using the blob name and returns 
            its contents as a pandas DataFrame. It does this by first obtaining 
            a blob client using the blob name and the container client, then downloading 
            the blob data and reading it as a string in memory using StringIO. The contents 
            of the string are then decoded as utf-8 and passed to pandas' read_csv method 
            to convert the data to a DataFrame.

        Args:
            This code assumes that the following variables are already defined:
            self: an instance of a class that has a container_client attribute which is an instance of BlobServiceClient in azure.storage.blob module
            blob_name: a string that specifies the name of the blob file to read
            
        Returns:
            A pandas DataFrame that contains the contents of the specified blob file.
        """
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        blob_csv = StringIO(blob_data.decode('utf-8'))
        return pd.read_csv(blob_csv)

    def get_filt_costByDate(self, desired_blob_files: dict) -> dict:
        '''
        Description/Functionality:
            This code defines a function that reads contents of desired blob files 
            and stores them in a dictionary called blobs_as_DFs, with keys as blob
            names and values as sum of their respective cost column. It also checks
            if the date in the file is between the 1st and 18th 
            of the month, and only includes the cost in the sum 
            if the date falls within this range.

        Args:
            This code assumes that the following variables are already defined:

            desired_blob_files: a dictionary containing the names of blob files to read
            self: an instance of a class that has a method named "read_blob" to read blob 
            files from an Azure Blob Storage container.
        
        Returns:
            blobs_as_DFs: a dictionary with keys as blob names and values as sum of their respective cost column, but only for the dates that fall within the 1st to 18th of the month.
        
        '''
        # Dictionary to store the file contents from the 1st to 18th of the month
        blobs_as_DFs = {}
        # Blob Counter 
        bCount = 1
        # Converting file contents to pandas DF and storing the sum of the cost column in the blobs_as_DFs dictionary
    
        eight_to_eighteen_sum = 0
        for blob in desired_blob_files:
            tofilterDF = self.read_blob(desired_blob_files[blob])[['CostInBillingCurrency', 'Date']]
            print(f'Blob {bCount} of {len(desired_blob_files)} read from {self.container_name}!\n')
            
            
            tofilterDF['Date'] = pd.to_datetime(tofilterDF['Date'])
            date_filter = (tofilterDF['Date'].dt.day >= 1) & (tofilterDF['Date'].dt.day <= 18)
            filtered_data = tofilterDF.loc[date_filter]
            eight_to_eighteen_sum = filtered_data['CostInBillingCurrency'].sum()
            blobs_as_DFs[blob] = eight_to_eighteen_sum
        
              
            bCount += 1
            
        return blobs_as_DFs
    
    def deliver_DF(self, oficial:dict, daily:dict) -> pd.DataFrame:
        """
        Description/Functionality:
            This code defines a method that takes two dictionaries, oficial and daily, as arguments 
            and returns a pandas DataFrame containing their values. The method first extracts the date 
            from the oficial dictionary using the keys method and initializes a new dictionary called 
            final_dict with a key-value pair that includes the extracted date and the corresponding value 
            from the oficial dictionary. It then updates final_dict with the contents of the oficial and 
            daily dictionaries and prints the resulting dictionary. Next, it sorts the key-value pairs in 
            final_dict in ascending order by key using the sorted method, converts the sorted dictionary to 
            a pandas DataFrame using the from_dict method, and saves the DataFrame to a CSV file named 
            "Final.csv". Finally, it prints the DataFrame.
        Args:

            self: an instance of a class that has an attribute called "read_blob" 
            which is a method that reads a blob file from Azure Blob Storage
            oficial: a dictionary that contains the costs of the month until yesterday
            daily: a dictionary that contains the costs of the current day
        Returns:
            A pandas DataFrame that contains the values of the oficial and daily dictionaries. 
            This DataFrame is also saved to a CSV file named "Final.csv" and printed.
        """
        official_date = str(list(oficial.keys())[0])
        
        final_dict = {f'Oficial: {official_date}': oficial.popitem()[1]}
        final_dict.update(oficial)
        final_dict.update(daily)
        final_dict = dict(sorted(final_dict.items()))
        finalDF = pd.DataFrame.from_dict(final_dict, orient='index',)
        finalDF.copy().to_csv(f'./{self.month}.csv')
        return finalDF
    
    def get_month_from_date(self, date:str)->str:
        month_list = ['January','February','March','April','May','June','July','August','September','October','November','December']
        strMonth = month_list[int(date.split('-')[1])-1]
        return strMonth
        

# # Example usage
# connection_string = 'BlobEndpoint=https://customerusagedata.blob.core.windows.net/;QueueEndpoint=https://customerusagedata.queue.core.windows.net/;FileEndpoint=https://customerusagedata.file.core.windows.net/;TableEndpoint=https://customerusagedata.table.core.windows.net/;SharedAccessSignature=sv=2021-12-02&ss=b&srt=sco&sp=rlx&se=2023-04-30T23:55:42Z&st=2023-03-27T15:55:42Z&spr=https&sig=5BT8GZ4nMbi2DWBCge992aXuAbXHFMMD0ikNXWGHr4c%3D'
# CLIENT_ID = 67968836
# requiredDates = ['2022-11-08','2022-12-08', '2023-01-08', '2023-02-08']

# for requiredDate in requiredDates:
#     # Connection to the blob 
#     monthly_usage = BlobHelper(connection_string, 'monthly-usage', 'new/monthlyEAUsage/')

#     # Filtered files in the blob
#     desired_files_Monthly = monthly_usage.get_dir_from_dateNID(requiredDate, CLIENT_ID)

#     # get filtered blobs costs on date monthly
#     filtered_blobs_on_date_monthly = monthly_usage.get_filt_costByDate(desired_files_Monthly)

#     # Connection to the blob 
#     daily_usage = BlobHelper(connection_string, 'daily-usage', 'new/dailyEAUsage/')

#     # Filtered files in the blob
#     desired_files_daily = daily_usage.get_dir_from_dateNID(requiredDate, CLIENT_ID)

#     # get filtered blobs costs on date daily
#     filtered_blobs_on_date_daily = daily_usage.get_filt_costByDate(desired_files_daily)

#     # Delivering the final DF
#     monthly_usage.deliver_DF(filtered_blobs_on_date_monthly, filtered_blobs_on_date_daily)



