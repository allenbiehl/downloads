from dataset_ageoff.processors.s3_bucket_ageoff.models import DatasetSource


class DatasetApiClient:
    """
    DatasetApiClient is a mock client that simulates the behavior of a real dataset API client. 
    It provides methods to retrieve dataset directories and their associated metadata.
    """
    def get_nfs_directories(self) -> list[DatasetSource]:
        """Mock database pull. Replace with your live DB connection query."""
        return [
            DatasetSource(
                dataset_name="project_A",
                source_name="projects-backup",   
                uri="nfs:///Users/evanbiehl/projects_backup",
                retention_days=400
            )
        ]

    def get_s3_buckets(self) -> list[DatasetSource]:
        """Mock database pull. Replace with your live DB connection query."""
        return [
            DatasetSource(
                dataset_name="project_A",
                source_name="bucket_1",
                uri="s3://enterprise-dataset-bucket-1",
                retention_days=400
            ),
            DatasetSource(
                dataset_name="project_A",
                source_name="bucket_2",
                uri="s3://enterprise-dataset-bucket-2",
                retention_days=3
            )
        ]
    
    def get_dataset_sources(self) -> list[DatasetSource]:
        """Mock database pull. Replace with your live DB connection query."""
        return self.get_nfs_directories() + self.get_s3_buckets()
