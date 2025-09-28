# Create: kafka/producers/debug_discovery.py
import requests
import json

def test_single_game(app_id=730):
    """Test fetching details for Counter-Strike 2"""
    print(f"Testing app ID {app_id}...")
    
    # Same API call as your discovery service
    params = {
        'appids': app_id,
        'filters': 'basic_info,price_overview,developers,publishers,platforms,categories,genres,screenshots,movies,recommendations,achievements,release_date,support_info,background,content_descriptors'
    }
    
    url = 'https://store.steampowered.com/api/appdetails'
    response = requests.get(url, params=params, timeout=10)
    
    print(f"Status code: {response.status_code}")
    print(f"URL called: {response.url}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Response keys: {list(data.keys())}")
        
        app_key = str(app_id)
        if app_key in data:
            print(f"App {app_id} found: success = {data[app_key]['success']}")
            
            if data[app_key]['success']:
                app_data = data[app_key]['data']
                print(f"Game name: '{app_data.get('name', 'NOT FOUND')}'")
                print(f"Game type: '{app_data.get('type', 'NOT FOUND')}'")
                print(f"Is free: {app_data.get('is_free', 'NOT FOUND')}")
                print(f"Description length: {len(app_data.get('short_description', ''))}")
                return app_data
            else:
                print(f"API returned success=False for app {app_id}")
        else:
            print(f"App {app_id} not found in response")
    else:
        print(f"HTTP error: {response.status_code}")
    
    return None

if __name__ == "__main__":
    # Test with CS2
    result = test_single_game(730)
    if result:
        print("\nSuccess! Data extraction working.")
    else:
        print("\nFailed to extract data.")