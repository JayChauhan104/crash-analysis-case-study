import json
import os

def load_config(config_file):
    """
    Load configuration from a JSON file.

    Parameters:
        config_file (str): The path to the JSON configuration file.

    Returns:
        dict: A dictionary containing the loaded configuration.

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
        json.JSONDecodeError: If the configuration file is not valid JSON.
    """
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

def df_write(dataframes: dict, config):
    """
    Write dataframes to output files according to the specified configuration.

    Parameters:
        dataframes (dict): A dictionary containing dataframes to be written.
        config (dict): A dictionary containing the configuration settings.

    Raises:
        KeyError: If required keys are missing in the configuration.
        ValueError: If an unsupported format is specified in the configuration.
    """
    output_dir = config.get("data_targets", {}).get("output_dir")
    if not output_dir:
        raise KeyError("Output directory not specified in the configuration")
    
    for output_name, output_config in config.get("data_targets", {}).items():
        if output_name != "output_dir":
            if output_name in dataframes:
                df = dataframes[output_name]
                output_format = output_config.get("format", "csv")
                output_filename = output_config.get("file_name")
                if not output_filename:
                    raise KeyError(f"File name not specified for output '{output_name}' in the configuration.")
                output_path = f"{output_dir}/{output_filename}"

                if  output_format == "csv":
                    df.repartition(1).write.csv(output_path, header=True, mode="overwrite")
                elif output_format == "parquet":
                    df.repartition(1).write.parquet(output_path, mode="overwrite")
                elif output_format == "orc":
                    df.repartition(1).write.orc(output_path, mode="overwrite")
                elif output_format == "json":
                    df.repartition(1).write.json(output_path, mode="overwrite")
                else:
                    raise ValueError(f"Unsupported format: {output_format}")
                
            else:
                print(f"Invalid key in output in json, please pass valid key!")