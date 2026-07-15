from cdet_etl.processors.dataflow_processor import DataFlowProcessor
from cdet_etl.readers import BaseReader
from cdet_etl.transformers import BaseTransformer
from cdet_etl.writers import BaseWriter
from cdet_etl.utils.class_utils import instantiate_class

class DataFlowFactory:
    """
    Reflective Execution Environment Provider.
    Dynamically maps configuration matrices to concrete pipeline instances.
    """
    @classmethod
    def create_processor(cls, dataflow_config: dict) -> DataFlowProcessor:
        """
        Factory Entry Point.
        Assembles and returns fully operational DataFlowProcessor environments.
        """
        return DataFlowProcessor(
            reader=cls.create_reader(dataflow_config),
            transformers=cls.create_transformers(dataflow_config),
            writer=cls.create_writer(dataflow_config)
        )

    @classmethod
    def create_reader(cls, dataflow_config: dict) -> list[BaseReader]:
        """
        Instantiate the pipeline Reader strategy component
        """
        config = dataflow_config["reader"]
        return instantiate_class(
            class_path=config["class_path"],
            properties=config.get("properties", {})
        )

    @classmethod
    def create_transformers(cls, dataflow_config: dict) -> list[BaseTransformer]:
        """
        Instantiate transformers dynamically, handling both string paths and dictionaries
        """
        transformers = []
        for config in dataflow_config.get("transformers", []):
            if isinstance(config, dict):
                class_path = config["class_path"]
                properties = config.get("properties", {})
                transformer = instantiate_class(class_path=class_path, properties=properties)
            else:
                transformer = instantiate_class(class_path=config)
            transformers.append(transformer)
        return transformers

    @classmethod
    def create_writer(cls, dataflow_config: dict) -> BaseWriter:
        """
        Instantiate the pipeline transactional Sink Writer component
        """
        config = dataflow_config["writer"]
        metadata_provider = None

        if "metadata" in config:
            metadata_cfg = config["metadata"]
            metadata_provider = instantiate_class(
                class_path=metadata_cfg["class_path"],
                properties=metadata_cfg.get("properties", {}),
            )

        return instantiate_class(
            class_path=config["class_path"],
            properties=config.get("properties", {}),
            metadata_provider=metadata_provider
        )
