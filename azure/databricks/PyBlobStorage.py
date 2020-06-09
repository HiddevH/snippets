from azure.storage.blob import BlockBlobService
import pandas as pd
from io import BytesIO, StringIO

class BlobStorage():

  def __init__(self, account_name:str, sas_token:str, container:str):

    self.block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)
    self.container = container

  def _read_stream_from_blob(func):
    def wrapped_func(self, file_name, args, *kwargs):
      with BytesIO() as stream:
        self.block_blob_service.get_blob_to_stream(self.container, file_name, stream)
        stream.seek(0)
        return func(self, stream, args, *kwargs)
    return wrapped_func

  def _write_stream_to_blob(func):
    def wrapped_func(self, file_name, args, *kwargs):
      with BytesIO() as stream:
        func(self, stream, args, *kwargs)
        self.block_blob_service.create_blob_from_bytes(self.container, file_name, stream.getvalue())
    return wrapped_func
  
  def _write_string_to_blob(func):
    def wrapped_func(self, file_name, *args, **kwargs):
      with StringIO() as stream:
        func(self, stream, *args, **kwargs)
        self.block_blob_service.create_blob_from_text(self.container, file_name, stream.getvalue())
    return wrapped_func
  
  @_write_stream_to_blob
  def write_df_to_parquet(self, file_name:str, df:pd.DataFrame):
    df.to_parquet(file_name, index=False, engine='pyarrow')
    
  @_write_string_to_blob
  def write_df_to_csv(self, file_name:str, df:pd.DataFrame, *args, **kwargs):
    df.to_csv(file_name, index=False, *args, **kwargs)
