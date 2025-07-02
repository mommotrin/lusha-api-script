import pandas as pd
import requests
import time
import os
from urllib.parse import urlparse 


INPUT_CSV_FILE = 'Companies_181876.csv' 
OUTPUT_CSV_FILE = 'companies_with_domains_lusha_batch.csv' 

# Column names in CSV:
COMPANY_NAME_COLUMN = 'Organization - Name' 
DOMAIN_COLUMN = 'Organization - Website (Lusha)' 
CLIENT_COMPANY_ID_COLUMN = 'Organization - ID' 

# --- Lusha API Configuration ---

LUSHA_API_KEY = os.getenv('LUSHA_API_KEY')
LUSHA_BATCH_ENRICHMENT_URL = 'https://api.lusha.com/bulk/company'

# Fallback if environment variable not set or not found (LESS SECURE!)
if LUSHA_API_KEY is None:
    print("WARNING: LUSHA_API_KEY environment variable not found.")
    print("For quick testing, you can uncomment and set it directly below (less secure):")
    
    if LUSHA_API_KEY is None or LUSHA_API_KEY == "ACTUAL_LUSHA_API_KEY_HERE":
        print("CRITICAL ERROR: Lusha API key is not set. Please set it as an environment variable or uncomment and paste it.")
        exit()

# --- Batching, Rate Limiting, and Retry Settings ---
BATCH_SIZE = 50            
REQUEST_DELAY_SECONDS = 2.0 
MAX_RETRIES = 3             
RETRY_DELAY_SECONDS = 15    

# --- Global Rate Limit Tracking Variables ---
lusha_daily_limit = "N/A"
lusha_daily_requests_left = "N/A"

print(f"Starting domain lookup for companies from: {INPUT_CSV_FILE}")

# --- Step 1: Load the CSV file ---
try:
    df = pd.read_csv(INPUT_CSV_FILE)
    print(f"Successfully loaded {len(df)} companies.")
except FileNotFoundError:
    print(f"Error: The file '{INPUT_CSV_FILE}' was not found. Please ensure it's in the same folder as this script.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the CSV: {e}")
    exit()

# --- Step 2: Prepare the DataFrame ---
if COMPANY_NAME_COLUMN not in df.columns:
    print(f"Error: '{COMPANY_NAME_COLUMN}' column not found. Please check your CSV header.")
    print("Existing columns are:", df.columns.tolist())
    exit()

if CLIENT_COMPANY_ID_COLUMN not in df.columns:
    print(f"Error: '{CLIENT_COMPANY_ID_COLUMN}' column not found. Please check your CSV header.")
    print("Existing columns are:", df.columns.tolist())
    exit()

if DOMAIN_COLUMN not in df.columns:
    df[DOMAIN_COLUMN] = '' 
    print(f"Added new column '{DOMAIN_COLUMN}' to store found domains.")
else:
    print(f"Using existing column '{DOMAIN_COLUMN}' for domains.")

df[DOMAIN_COLUMN] = df[DOMAIN_COLUMN].astype(str).replace('nan', '')


# --- Function to call Lusha Batch API ---
def get_domains_from_lusha_batch(companies_batch_data, api_key, url):
    """
    Makes a batch POST request to the Lusha API to find company domains.

    Args:
        companies_batch_data (list): A list of tuples, each containing (client_id, company_name, pandas_index).
        api_key (str): Your Lusha API key.
        url (str): The Lusha batch API endpoint URL.

    Returns:
        dict: A dictionary mapping original pandas_index to found domain or an error string.
              Returns {"error": "ERROR_MESSAGE"} on critical failures.
    """
    global lusha_daily_limit, lusha_daily_requests_left 

    headers = {
        'api_key': api_key, 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    request_body = {
        "companies": []
    }
    id_to_pandas_index_map = {}

    for client_id, company_name, pandas_index in companies_batch_data:
        if pd.isna(client_id) or str(client_id).strip() == '':
            print(f"  > Skipping company due to missing Client ID at pandas index {pandas_index}.")
            continue
        if company_name and str(company_name).strip() != '':
            lusha_id = str(client_id).strip()
            request_body["companies"].append({
                "id": lusha_id,
                "name": str(company_name).strip()
            })
            id_to_pandas_index_map[lusha_id] = pandas_index
        else:
            print(f"  > Skipping empty company name in batch for Client ID: {client_id}")

    if not request_body["companies"]:
        print("  > No valid companies to process in this batch.")
        return {}

    print(f"  > Sending batch of {len(request_body['companies'])} companies to Lusha API...")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, json=request_body, timeout=20)

            # --- Extract and update rate limit headers ---
            lusha_daily_limit = response.headers.get('x-rate-limit-daily', lusha_daily_limit)
            lusha_daily_requests_left = response.headers.get('x-daily-requests-left', lusha_daily_requests_left)
            
            try:
                lusha_daily_limit = int(lusha_daily_limit)
            except ValueError:
                pass
            try:
                lusha_daily_requests_left = int(lusha_daily_requests_left)
            except ValueError:
                pass
            # --- End rate limit header extraction ---

            if response.status_code in [200, 201]:
                try:
                    data = response.json()
                except requests.exceptions.JSONDecodeError as e:
                    print(f"  > CRITICAL JSON ERROR: Could not decode JSON from response: {e}")
                    print(f"  > Full Response Text (for debugging): {response.text}")
                    return {"error": "JSON_DECODE_ERROR"}

                results = {}
                if data:
                    for returned_lusha_id, company_data_from_lusha in data.items():
                        found_domain = "Not Found"

                        if isinstance(company_data_from_lusha, dict) and company_data_from_lusha.get('code') == 3 and company_data_from_lusha.get('name') == 'EMPTY_DATA':
                            found_domain = "Not Found (Lusha Empty Data)"
                        elif isinstance(company_data_from_lusha, dict):
                            if 'fqdn' in company_data_from_lusha and company_data_from_lusha['fqdn']:
                                found_domain = company_data_from_lusha['fqdn']
                            elif 'domain' in company_data_from_lusha and company_data_from_lusha['domain']:
                                found_domain = company_data_from_lusha['domain']
                            elif 'website' in company_data_from_lusha and company_data_from_lusha['website']:
                                parsed_url = urlparse(company_data_from_lusha['website'])
                                if parsed_url.netloc:
                                    found_domain = parsed_url.netloc
                                else:
                                    found_domain = "No Domain in Website URL"

                        if returned_lusha_id and returned_lusha_id in id_to_pandas_index_map:
                            original_pandas_index = id_to_pandas_index_map[returned_lusha_id]
                            results[original_pandas_index] = found_domain
                        else:
                            print(f"  > Warning: Lusha returned an ID '{returned_lusha_id}' not found in our original batch map.")
                else:
                    print(f"  > Warning: Lusha returned an empty data object for a {response.status_code} response.")
                    for client_id, _, pandas_index in companies_batch_data:
                        results[pandas_index] = "API_RESPONSE_EMPTY_DATA"

                return results

            elif response.status_code == 429:
                print(f"  > Rate limit hit. Retrying in {RETRY_DELAY_SECONDS} seconds (Attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            elif response.status_code == 401:
                print(f"  > Unauthorized (401). Check your Lusha API key. Critical error, exiting batch processing.")
                return {"error": "AUTH_ERROR"}

            else: # Catch any other non-success status codes, like 403
                print(f"  > API error for batch: Status {response.status_code}, Response: {response.text}")
                
                if "Lusha FireWall" in response.text:
                    print("  > Lusha FireWall blocked access. This is likely due to rate limits or account restrictions.")
                    return {"error": "LUSHA_FIREWALL_BLOCK"}
                return {"error": f"API_ERROR_STATUS_{response.status_code}"}

        except requests.exceptions.Timeout:
            print(f"  > Request timed out for batch. Retrying (Attempt {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(RETRY_DELAY_SECONDS)
        except requests.exceptions.RequestException as e:
            print(f"  > Network or request error for batch: {e}. Retrying (Attempt {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            print(f"  > An unexpected error occurred for batch: {e}")
            return {"error": "UNKNOWN_ERROR"}

    print(f"  > Failed to get domains for batch after {MAX_RETRIES} attempts.")
    return {"error": "Failed After Retries"}

# --- Step 3: Loop through companies in batches and process ---
companies_to_process_batch = []
for index, row in df.iterrows():
    company_name = row[COMPANY_NAME_COLUMN]
    existing_domain = row[DOMAIN_COLUMN]
    client_company_id = row[CLIENT_COMPANY_ID_COLUMN]

    # Stop if daily requests are exhausted
    if isinstance(lusha_daily_requests_left, int) and lusha_daily_requests_left <= 0:
        print(f"\nDAILY LUSHA API QUOTA EXHAUSTED (0 requests left). Stopping script gracefully.")
        break # Exit the loop, will save processed data

    if (not existing_domain or str(existing_domain).strip() == '') and \
       (company_name and str(company_name).strip() != ''):
        companies_to_process_batch.append((client_company_id, company_name, index))

    if len(companies_to_process_batch) >= BATCH_SIZE or \
       (index == len(df) - 1 and len(companies_to_process_batch) > 0):

        batch_results = get_domains_from_lusha_batch(companies_to_process_batch, LUSHA_API_KEY, LUSHA_BATCH_ENRICHMENT_URL)

        if "error" in batch_results:
            error_message = batch_results['error']
            print(f"  > Critical error encountered in batch: {error_message}. Stopping script.")
            print(f"  > Current Lusha Daily Limit: {lusha_daily_limit}, Requests Left: {lusha_daily_requests_left}")

            for _, _, original_pandas_idx in companies_to_process_batch:
                df.loc[original_pandas_idx, DOMAIN_COLUMN] = error_message
            df.to_csv(OUTPUT_CSV_FILE, index=False)
            exit() # Exit the script on critical errors

        for original_pandas_index, domain_found in batch_results.items():
            df.loc[original_pandas_index, DOMAIN_COLUMN] = domain_found

        companies_to_process_batch = []
        print(f"  > Pausing for {REQUEST_DELAY_SECONDS} seconds...")
        print(f"  > Current Lusha Daily Limit: {lusha_daily_limit}, Requests Left: {lusha_daily_requests_left}")
        time.sleep(REQUEST_DELAY_SECONDS)

# --- Step 4: Save the updated DataFrame to a new CSV file ---
df.to_csv(OUTPUT_CSV_FILE, index=False)
print(f"\nFinished processing. Results saved to: {OUTPUT_CSV_FILE}")
print(f"Final Lusha Daily Limit: {lusha_daily_limit}, Requests Left: {lusha_daily_requests_left}")
