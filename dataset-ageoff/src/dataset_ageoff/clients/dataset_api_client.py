from dataset_ageoff.inventory.models import DatasetDirectory


class DatasetApiClient:
    """
    DatasetApiClient is a mock client that simulates the behavior of a real dataset API client. 
    It provides methods to retrieve dataset directories and their associated metadata.
    """
    def get_directories(self) -> list[DatasetDirectory]:
        """Mock database pull. Replace with your live DB connection query."""
        return [
            DatasetDirectory(
                id=1001,
                name="project_A",
                path="/Users/evanbiehl/Projects/etl",
                age_off_days=0
            ),
            # DatasetDirectory(
            #     id=1002,
            #     name="project_B",
            #     path="/Users/evanbiehl/Projects",
            #     age_off_days=180
            # ),
            # DatasetDirectory(
            #     id=1003,
            #     name="project_C",
            #     path="/Users/evanbiehl/Projects",
            #     age_off_days=360
            # )
        ]
    