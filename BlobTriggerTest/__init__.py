import logging
import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.functions import get_app_setting




COLORS = {
    'BLACK': '\033[30m',
    'RED': '\033[31m',
    'GREEN': '\033[32m',
    'YELLOW': '\033[33m',
    'BLUE': '\033[34m',
    'MAGENTA': '\033[35m',
    'CYAN': '\033[36m',
    'WHITE': '\033[37m',
    'RESET': '\033[0m'
}

class TriggerProcessorHelper:
    def __init__(self, catched_blob: func.InputStream) -> None:
        self.catched_blob = catched_blob
        self.blob_DF = pd.read_csv(self.catched_blob)
    
    def logInformation(self):
        logging.info(
            f"\nPython blob trigger function processed blob \n"
            f"Name: {self.catched_blob.name}\n"
            f"Blob Size: {self.catched_blob.length} bytes\n"
            f"closed: {self.catched_blob.closed}\n"
        )

    def setNewBlobName(self, df: pd.DataFrame):
        
        first_sub_id = df['SubscriptionId'].iloc[0]
        first_met_id = df['MeterId'].iloc[0]
        first_date_parts = df['Date'].iloc[0].split('/')
        
        #Suscription ID check
        if not df['SubscriptionId'].eq(first_sub_id).all():
            raise Exception('Not all SubscriptionId\'s are the same')
        #Meter ID check
        if not df['MeterId'].eq(first_met_id).all():
            raise Exception('Not all MeterId\'s are the same')
        #Date check
        if not (df['Date'].str.split('/', expand=True).iloc[:, [0, 2]] == first_date_parts[0::2]).all(axis=None):
            raise Exception('Not all in the same month and year')
        
        return f"{first_sub_id}_{first_date_parts[2]}{first_date_parts[0]}_{first_met_id}.csv"

    def groupIDs(self):
        df = self.blob_DF
        stack = df['MeterId'].unique().tolist()
        while stack:
            current_id = stack.pop()
            filtered_df = df[df['MeterId'] == current_id]
            new_name = self.setNewBlobName(filtered_df)
            logging.info(f'\n{COLORS["YELLOW"]}--------------------------------------------------------------------------------------------------\n{COLORS["YELLOW"]}Uploading: {COLORS["RESET"]} {COLORS["GREEN"]} {new_name} {COLORS["RESET"]}\n{COLORS["YELLOW"]}Destination: {COLORS["RESET"]} : some blob \n{COLORS["YELLOW"]}--------------------------------------------------------------------------------------------------\n')
            # TODO : Save file to blob storage
            # filtered_df.to_csv(new_name, index=False)
 
    


def main(myblob: func.InputStream):
    
    
    
    helper = TriggerProcessorHelper(myblob)
    helper.logInformation()
    helper.groupIDs()



    
    

