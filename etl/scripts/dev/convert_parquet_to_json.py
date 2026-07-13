import pandas as pd

input_file = '/Users/evanbiehl/Projects/etl/.data/input/part-20260713021815000479-675eca.parquet'
output_file = '/Users/evanbiehl/Projects/etl/.data/output/output.json'

# Read the Parquet file into a DataFrame
df = pd.read_parquet(input_file)

# Export to a standard JSON array of records
df.to_json(output_file, orient='records', lines=True)