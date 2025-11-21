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
    
    print("Generating mock Dutchie POS export CSV files...")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Locations: {', '.join(LOCATIONS.keys())}\n")
    
    for location_name in LOCATIONS.keys():
        print(f"Generating data for {location_name}...")
        mock_data = generate_mock_data(location_name, start_date, end_date)
        print()

def generate_single_location_csv(location_name, days=7):
    """Generate CSV file for a specific location"""
    if location_name not in LOCATIONS:
        print(f"Error: Location '{location_name}' not found in config.py")
        print(f"Available locations: {', '.join(LOCATIONS.keys())}")
        print(f"\nTo add a new location, edit config.py and add to the LOCATIONS dict.")
        return False
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"Generating mock data for {location_name}...")
    print(f"Date range: {start_date.date()} to {end_date.date()}\n")
    
    mock_data = generate_mock_data(location_name, start_date, end_date)
    return True

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) == 1:
        # No arguments - generate for all locations (7 days)
        generate_all_location_csvs()
        print("\nAll CSV files generated successfully in the 'mock_data' folder")
        
    elif len(sys.argv) == 2:
        # One argument - specific location (7 days)
        location = sys.argv[1]
        if generate_single_location_csv(location):
            print(f"\nCSV file generated for {location} in the 'mock_data' folder")
        
    elif len(sys.argv) == 3:
        # Two arguments - specific location and days
        location = sys.argv[1]
        try:
            days = int(sys.argv[2])
            if generate_single_location_csv(location, days):
                print(f"\n {days} days of data generated for {location} in the 'mock_data' folder")
        except ValueError:
            print("Error: Days must be a number")
            print("Usage: python generate_mock_csv.py Columbus")
    
    print("\nYou can now upload these files through the dashboard interface.")
