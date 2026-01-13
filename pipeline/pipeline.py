import sys
import pandas as pd

print('arguments', sys.argv)

try:
    month = int(sys.argv[1])
except ValueError:
    print("Error: el argumento <month> debe ser un numero entero.")
    sys.exit(1)


df = pd.DataFrame({"day":[1,2], "num_passengers": [4,5]})

df['month'] = month

print(df.head())

df.to_parquet(f"output_{month}.parquet")


print(f'Hello pipeline, month = {month}')