from pyarrow import json
import pyarrow.parquet as pq

input_file = '/Users/evanbiehl/Projects/etl/.data/input/example.json'
output_file = '/Users/evanbiehl/Projects/etl/.data/output/output.parquet'

# Read the JSON file into an Arrow Table
table = json.read_json(input_file)

# Write the Table directly to Parquet
pq.write_table(table, output_file)