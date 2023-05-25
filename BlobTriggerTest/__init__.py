import logging

import azure.functions as func


def main(myblob: func.InputStream):
    logging.info(
            f"Python blob trigger function processed blob \n"
            f"Name: {myblob.name}\n"
            f"Blob Size: {myblob.length} bytes\n"
            f"closed: {myblob.closed}\n"
            f"closed: {myblob.closed}\n"
            f"content: \n{myblob.read()}\n"
        )
