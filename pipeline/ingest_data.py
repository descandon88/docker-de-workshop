#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# Read a sample of the data
parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']



# df = pd.read_csv(url)

def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print(f"Table {target_table} created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)}")

    print(f'done ingesting to {target_table}')











def main():

    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'
    chunksize = 100000

    year = 2021

    month = 1

    target_table = 'yellow_taxi_data'

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

   # df_parsed_date = pd.read_csv(url, dtype=dtype, parse_dates = parse_dates )
    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

if __name__ == '__main__':
    main()
# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


#df_parsed_date.head(0)



# Get DDL Schema for the database:
#print(pd.io.sql.get_schema(df_parsed_date, name='yellow_taxi_data', con=engine))



# df_parsed_date.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# df_iter = pd.read_csv(
#    url, 
#    dtype=dtype, 
#    iterator=True,
#    chunksize=100000,
#    parse_dates = parse_dates 
# )




# df = next(df_iter)



# get_ipython().system('uv add stqdm')





# for df_chunk in tqdm(df_iter):
 #   df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
  #  print(len(df_chunk))





