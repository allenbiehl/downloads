from cdet_etl.processors.dataflow_registry import DataFlowRegistry
from cdet_etl.readers.s3.s3_data_source import S3DataSource

if __name__ == "__main__":
    registry = DataFlowRegistry()

    messages = [
        #{"dataflow_id": "stage-1", "uri": "s3://input/json_format_1.json"},
        # {"dataflow_id": "stage-1", "uri": "s3://input/json_format_2.json"},
        # {"dataflow_id": "stage-1", "uri": "s3://input/json_format_3.json"},
        # {"dataflow_id": "stage-2", "uri": "s3://stage-1/files/2026/01/01/part-20260715024225275510-58357a.parquet"},
        {"dataflow_id": "stage-3", "uri": "s3://stage-1/files/2026/01/01/part-20260715024225275510-58357a.parquet"},
        # {"dataflow_id": "dataflow-1", "uri": "s3://input/mock_geo_data_50mb.json"},
        # {"dataflow_id": "dataflow-2", "uri": "s3://input/mock_geo_data_100mb.json"}
    ]

    for message in messages:
        dataflow_id = message["dataflow_id"]
        source_uri = message["uri"]
        processor = registry.get_processor(dataflow_id)
        processor.execute(source=S3DataSource(uri=source_uri))
