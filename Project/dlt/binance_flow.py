import pandas as pd
import numpy as np
import json
from datetime import datetime
import duckdb
from typing import Any, List, Optional
#import pyarrow
import dlt
from dlt.destinations import filesystem #for GCS

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator, BasePaginator
from dlt.sources.helpers.requests import Response, Request

#source for custom paginator: https://dlthub.com/docs/general-usage/http/rest-client#paginators
#Chatgpt helped me build it based on that page.
class TimeRangePaginator(BasePaginator):
    def __init__(self, start_time: int, end_time: int, interval_ms: int):
        super().__init__()
        self.current_time = start_time
        self.end_time = end_time
        self.interval_ms = interval_ms

    def init_request(self, request: Request) -> None:
        """Initialize the request with the first startTime and endTime"""
        self.update_request(request)

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Update the pagination state based on response data"""
        if not response.json():  # Stop if no more data
            self._has_next_page = False
        else:
            self.current_time += self.interval_ms

    def update_request(self, request: Request) -> None:
        """Update the request parameters with the current time range"""
        if request.params is None:
            request.params = {}

        next_end_time = min(self.current_time + self.interval_ms, self.end_time)
        request.params['startTime'] = self.current_time
        request.params['endTime'] = next_end_time

start_time = int(datetime.utcnow().timestamp())*1000-60*1000*10
print(start_time)
end_time = int(datetime.utcnow().timestamp())*1000-60*1000*2
print(end_time)


@dlt.resource(name="aggtrades", write_disposition="append") #aggtrades will be the table name
def binance_api(start_timem,end_time):
    client = RESTClient(
        base_url="https://data-api.binance.vision"
        ,paginator=TimeRangePaginator(
        #start_time=1672531200000,  # Start time in milliseconds (e.g., 2023-01-01)
        #end_time=1672617600000,    # End time in milliseconds (e.g., 2023-01-02)
        start_time=start_time,  # Start time in milliseconds (e.g., 2023-01-01)
        end_time=end_time,    # End time in milliseconds (e.g., 2023-01-02)
        interval_ms=60  * 1000  # 1-minute interval
        )
        
    )

    for page in client.paginate("/api/v3/aggTrades?symbol=BTCUSDT"):
        yield page

## define new dlt pipeline to test with duckdb
#pipeline = dlt.pipeline(
#    destination="duckdb", #database technology
#    pipeline_name='binance', #database name in the destination
#    dataset_name='aggtrade' #dataset name in the destination
#)

# run the pipeline with the new resource
#load_info = pipeline.run(binance_api, write_disposition="replace")
#print(load_info)

## define new dlt pipeline for Google Cloud Storage
pipeline = dlt.pipeline(
    destination="filesystem", #database technology
    pipeline_name='binance', #database name in the destination
    dataset_name='aggtrade' #dataset name in the destination
)

# run the pipeline with the new resource
load_info = pipeline.run(binance_api(start_time,end_time), write_disposition="append", # appends create a new file each tim
                         table_name='binance_data') #table name corresponds to a subfolder inside dataset_name
print(load_info)

# explore loaded data
#pipeline.dataset(dataset_type="default").aggtrades.df()


# explore loaded data
#pipeline.dataset(dataset_type="default").aggtrades.df()

# we reuse the pipeline instance below and load to the same dataset as data
#pipeline.run([load_info], table_name="_load_info")

# Get the trace of the last run of the pipeline
# The trace contains timing information on extract, normalize, and load steps
#trace = pipeline.last_trace

# Load the trace information into a table named "_trace" in the destination
#pipeline.run([trace], table_name="_trace")