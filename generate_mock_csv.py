"""
Generate mock Dutchie POS CSV export files for testing
This creates realistic POS export files that can be uploaded to the dashboard

"""
import sys
from datetime import datetime, timedelta
from data_ingestion import generate_mock_data
from config import LOCATIONS

def generate_all_location_csvs(days=7):
    """Generate CSV files for all locations configured in config.py"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    for location_name in LOCATIONS.keys():
        mock_data = generate_mock_data(location_name, start_date, end_date)

def generate_single_location_csv(location_name, days=7):
    """Generate CSV file for a specific location"""
    if location_name not in LOCATIONS:
        print(f"Error: Location '{location_name}' not found in config.py")
        print(f"Available locations: {', '.join(LOCATIONS.keys())}")
        return False
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    mock_data = generate_mock_data(location_name, start_date, end_date)
    return True

if __name__ == "__main__":
    if len(sys.argv) == 1:
        generate_all_location_csvs()
        
    elif len(sys.argv) == 2:
        location = sys.argv[1]
        generate_single_location_csv(location)
        
    elif len(sys.argv) == 3:
        location = sys.argv[1]
        try:
            days = int(sys.argv[2])
            generate_single_location_csv(location, days)
        except ValueError:
            print("Error: Days must be a number")
            print("Usage: python generate_mock_csv.py Columbus")
