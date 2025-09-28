# Create: kafka/producers/debug_discovery_simple.py
import requests
import json

def test_simple_api(app_id=730):
    """Test with minimal parameters"""
    print(f"Testing simple API call for app {app_id}...")
    
    # Simplified API call - no filters
    params = {'appids': app_id}
    
    url = 'https://store.steampowered.com/api/appdetails'
    response = requests.get(url, params=params, timeout=10)
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        app_key = str(app_id)
        
        if app_key in data and data[app_key]['success']:
            app_data = data[app_key]['data']
            print(f"Game name: '{app_data.get('name', 'NOT FOUND')}'")
            print(f"Game type: '{app_data.get('type', 'NOT FOUND')}'")
            print(f"Developers: {app_data.get('developers', 'NOT FOUND')}")
            print(f"Publishers: {app_data.get('publishers', 'NOT FOUND')}")
            
            # Show what keys are actually available
            print(f"Available keys: {list(app_data.keys())[:10]}...")  # First 10 keys
            return True
        else:
            print("No success in response")
    
    return False

if __name__ == "__main__":
    test_simple_api(730)