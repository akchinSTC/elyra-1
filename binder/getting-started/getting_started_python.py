import io
import pandas as pd
import requests


def create_df_from_remote_csv(url):
    """
    Loads url and creates a Pandas DataFrame if url identifies an existing CSV file.
    If an error is encountered None is returned.
    """
    if url is None:
        return None
    response = requests.get(url)
    if response.status_code == 200:
        if response.headers['content-type'] == "text/csv":
            response.encoding = 'utf-8'
            data = pd.read_csv(io.StringIO(response.text))
            return data
        else:
            print('Error. The file is encoded using unsupported content-type {}'
                  .format(response.headers['content-type']))
    else:
        print('Error. The file could not be downloaded. Returned HTTP status code: {}'
              .format(response.status_code))

    return None


# Load a CSV data set into a Pandas DataFrame
df = create_df_from_remote_csv("https://datahub.io/machine-learning/iris/r/iris.csv")
if df is not None:
    # Print first few data rows
    print(df.head(10))
else:
    print("Data file couldn't be loaded into a DataFrame.")
