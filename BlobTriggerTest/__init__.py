import logging
import azure.functions as func
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from io import StringIO


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

    def groupIDsToBS(self):
        df = self.blob_DF
        stack = df['MeterId'].unique().tolist()
        while stack:
            current_id = stack.pop()
            filtered_df = df[df['MeterId'] == current_id]
            new_name = self.setNewBlobName(filtered_df)
            logging.info(f'\n{COLORS["YELLOW"]}--------------------------------------------------------------------------------------------------\n{COLORS["YELLOW"]}Uploading: {COLORS["RESET"]} {COLORS["GREEN"]} {new_name} {COLORS["RESET"]}\n{COLORS["YELLOW"]}Destination: {COLORS["RESET"]} : some blob \n{COLORS["YELLOW"]}--------------------------------------------------------------------------------------------------\n')
            
            # Getting the connection string from the key vault
            vault_url = "https://triggertestkeyvault27.vault.azure.net/"
            credential = DefaultAzureCredential()
            secret_client = SecretClient(vault_url=vault_url, credential=credential)
            secret_name = "connectionStringTest"
            second_secret_name = 'cname'
            secret = secret_client.get_secret(secret_name)
            second_secret = secret_client.get_secret(second_secret_name)
            connectionStringSecret = secret.value
            containerNameSecret = second_secret.value

            # using IO to save the DF to a StringIO object
            csv_data = StringIO()
            filtered_df.to_csv(csv_data, index=False)
            csv_data.seek(0)

            # Uploading data from to blob storage
            
            blob_service_client = BlobServiceClient.from_connection_string(connectionStringSecret)
            blob_client = blob_service_client.get_blob_client(containerNameSecret, new_name)
            blob_client.upload_blob(csv_data.getvalue(), overwrite=True)


    


def main(myblob: func.InputStream):
    helper = TriggerProcessorHelper(myblob)
    helper.groupIDsToBS()

    



    


    
    

