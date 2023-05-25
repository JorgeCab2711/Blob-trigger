import azure.functions as func

class AzureBlobTriggerProcessor:
    def __init__(self, catched_blob: func.InputStream) -> None:
        self.catched_blob = catched_blob
    


