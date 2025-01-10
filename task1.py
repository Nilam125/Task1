import pandas as pd
import requests

# Step 1: Extract
def extract_data(api_url: str):
    """
    Fetch data from the API.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        print("Data extracted successfully from API.")
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Step 2: Transform
def transform_data(data: pd.DataFrame):
    """
    Clean and preprocess the data.
    """
    try:
        # Select relevant columns (if applicable)
        if "body" in data.columns:
            data = data.rename(columns={"body": "content"})
        
        # Add a new column (e.g., word count in the content)
        if "content" in data.columns:
            data["word_count"] = data["content"].apply(lambda x: len(x.split()))
        
        # Drop unnecessary columns (if any)
        data = data.drop(columns=["id"], errors="ignore")
        
        print("Data transformed successfully.")
        return data
    except Exception as e:
        print(f"Error transforming data: {e}")
        return None

# Step 3: Load
def load_data(data: pd.DataFrame, output_file: str):
    """
    Save the processed data to a CSV file.
    """
    try:
        data.to_csv(output_file, index=False)
        print(f"Data saved successfully to {output_file}.")
    except Exception as e:
        print(f"Error saving data: {e}")

# Main Pipeline
def main_pipeline(api_url: str, output_file: str):
    """
    Execute the ETL pipeline.
    """
    # Step 1: Extract
    data = extract_data(api_url)
    if data is None:
        return
    
    # Step 2: Transform
    data = transform_data(data)
    if data is None:
        return
    
    # Step 3: Load
    load_data(data, output_file)

# Example Usage
if __name__ == "__main__":
    api_url = "https://jsonplaceholder.typicode.com/posts"
    output_file = "api_data.csv"
    main_pipeline(api_url, output_file)
