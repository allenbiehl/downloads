import json

class XmlSchemaConfigLoader:
    """
    Utility class dedicated to loading, parsing, and validating XML schema configurations.
    Isolated from cloud dependencies to ensure pure, lightning-fast unit testing.
    """
    
    @staticmethod
    def load_configuration(config_target: str) -> tuple:
        """
        Parses a configuration target which can be an inline JSON string or a local file path.
        Returns:
            tuple: (field_names_list, type_mappings_dict)
        """
        if not config_target or not isinstance(config_target, str) or not config_target.strip():
            return None, {}

        config_target = config_target.strip()

        try:
            # Scenario A: The string is an inline JSON object block
            if config_target.startswith("{"):
                config = json.loads(config_target)
                return config.get("fields"), config.get("types", {})
            
            # Scenario B: The string represents a local configuration file path
            with open(config_target, "r", encoding="utf-8") as f:
                config = json.load(f)
                return config.get("fields"), config.get("types", {})
                
        except FileNotFoundError:
            print(f"[CONFIG WARNING] Configuration file '{config_target}' not found. Falling back to auto-detect.")
            return None, {}
            
        except (json.JSONDecodeError, IOError) as e:
            print(f"[CONFIG WARNING] Failed to parse schema configuration '{config_target}': {e}. Falling back to auto-detect.")
            return None, {}
