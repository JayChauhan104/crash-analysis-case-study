class DFStore:
    """
    A class to store and manage DataFrames.

    Attributes:
        __dfs (dict): A dictionary to store DataFrames.
        __spark_app: The SparkSession object.
    """
    __dfs = {}
    __spark_app = None
    def __init__(self, spark_app=None, dfs=None):
        """
        Initialize the DFStore.

        Parameters:
            spark_app: The SparkSession object.
            dfs (dict): A dictionary containing DataFrames to initialize the store.
        """
        self.__spark_app = spark_app
        if dfs:
            self.__dfs = dfs

    def __get_err_str(self, df_name):
        """
        Generate an error string for a DataFrame not found error.

        Parameters:
            df_name (str): The name of the DataFrame.

        Returns:
            str: The error string.
        """
        return f"Given df with name '{df_name}' not found"

    def get_df(self, df_name):
        """
        Retrieve a DataFrame from the store.

        Parameters:
            df_name (str): The name of the DataFrame.

        Returns:
            DataFrame or None: The retrieved DataFrame, or None if not found.
            str or None: An error message if the DataFrame is not found, or None if found.
        """
        df = self.__dfs.get(df_name)

        if not df:
            return None, self.__get_err_str(df_name)
        return df, None
    
    def set_df(self, df_name, value):
        """
        Set a DataFrame in the store.

        Parameters:
            df_name (str): The name of the DataFrame.
            value: The DataFrame object to be stored.
        """
        self.__dfs[df_name] = value
    
    def load_df_from_csv(self, source, df_name, header=False, inferSchema=False):
        """
        Load a DataFrame from a CSV file and store it.

        Parameters:
            source (str): The path to the CSV file.
            df_name (str): The name to assign to the loaded DataFrame.
            header (bool): Whether the CSV file has a header row. Default is False.
            inferSchema (bool): Whether to infer the schema of the CSV data. Default is False.
        """
        df = self.__spark_app.read.csv(source, header=header, inferSchema=inferSchema)
        self.set_df(df_name, df)
