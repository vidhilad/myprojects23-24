import requests
import pandas as pd
import time

# Function to handle API requests with rate limit handling
def api_request_with_retry(url, headers, timeout=60, max_retries=3):
    retries = 0
    while retries < max_retries:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:  # Rate limit error
            retry_after = int(response.headers.get('Retry-After', 1))
            print(f"Rate limit hit. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
        retries += 1
    raise Exception("Max retries reached. API request failed.")

# Function to get Spotify access token
def get_spotify_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }, timeout=60)
    
    if auth_response.status_code != 200:
        raise Exception("Failed to authenticate with Spotify API: " + auth_response.text)
    
    auth_data = auth_response.json()
    return auth_data['access_token']

# Function to search for a track and get its ID
def search_track(track_name, artist_name, token):
    query = f"{track_name} artist:{artist_name}"
    url = f"https://api.spotify.com/v1/search?q={query}&type=track"
    headers = {'Authorization': f'Bearer {token}'}
    response = api_request_with_retry(url, headers)
    json_data = response.json()
    
    try:
        first_result = json_data['tracks']['items'][0]
        return first_result['id']
    except (KeyError, IndexError):
        return None

# Function to get track details, specifically the album cover image URL
def get_track_details(track_id, token):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {'Authorization': f'Bearer {token}'}
    response = api_request_with_retry(url, headers)
    json_data = response.json()
    
    if 'album' in json_data and 'images' in json_data['album'] and json_data['album']['images']:
        return json_data['album']['images'][0]['url']
    return None

# Spotify API Credentials (use environment variables)
client_id = '771617d269874eb6b68098ff64d8de7b'
client_secret = '54a4937f42494f62b3f7303c152790bc'

# Get Access Token
access_token = get_spotify_token(client_id, client_secret)

# Read your DataFrame
df_spotify = pd.read_csv('Spotify2023.csv', encoding='ISO-8859-1')

# Loop through each row to get track details and add to DataFrame
for i, row in df_spotify.iterrows():
    try:
        if pd.isna(row['track_name']) or pd.isna(row['artist_name']):
            print(f"Missing data for row {i}: {row}")
            df_spotify.at[i, 'image_url'] = None
            continue
        
        track_id = search_track(row['track_name'], row['artist_name'], access_token)
        if track_id:
            image_url = get_track_details(track_id, access_token)
            df_spotify.at[i, 'image_url'] = image_url
        else:
            print(f"No track found for row {i}: {row}")
            df_spotify.at[i, 'image_url'] = None
        
        time.sleep(1)  # Pause to avoid hitting rate limits
    except Exception as e:
        print(f"Error processing row {i} ({row['track_name']} by {row['artist_name']}): {e}")
        df_spotify.at[i, 'image_url'] = None

# Save the updated DataFrame
df_spotify.to_csv('spotify2023URL.csv', index=False)




































