import os
import csv
import threading
from datetime import datetime, timedelta
from queue import Queue

# These are the constants for trading hours, they are used through out
TRADING_START = datetime.strptime("09:30", "%H:%M").time()
TRADING_END = datetime.strptime("16:00", "%H:%M").time()

# This is the queue to store loaded data from CSV files
data_queue = Queue()

# This is the class to handle all the functions
class DataProcessor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.all_data = []

    def load_csv_files(self):
        # Iterates through each file in data_dir and stores them in a list named files
        files = [f for f in os.listdir(self.data_dir)]
        # Threads worker function
        def worker(file):
            file_path = os.path.join(self.data_dir, file)
            try:
                with open(file_path, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        # Adding each row of trade data to the queue
                        data_queue.put(row)
            except Exception as e:
                print(f"Error loading {file}: {e}")

        # Create and start threads for each file
        threads = []
        for file in files:
            thread = threading.Thread(target=worker, args=(file,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        print(f"Total rows loaded into queue: {data_queue.qsize()}")
        print(f"Finished loading {len(files)} files.")

    def clean_data(self):
        # Clean data by removing missing values, duplicates, prices that are negetive, and timestamps that aren't in regular trading hours
        cleaned_data = []
        duplicateChecker = set()

        while not data_queue.empty():
            row = data_queue.get()

            # Collecting each piece of data
            timestamp = row['Timestamp']
            price = row['Price']
            size = row['Size']

            # Skiping rows with missing values
            if not timestamp or not price or not size:
                continue

            # Convert timestamp to datetime and check trading hours
            try:
                # Parsing timestamp to include milliseconds
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
                
                if not (TRADING_START <= dt.time() <= TRADING_END):
                    # Skip rows outside regular trading hours
                    continue
            except ValueError:
                # Print invalid timestamp if there's an issue so we can debug
                print(f"Invalid timestamp format: {timestamp}")
                continue

            # Check for duplicates
            if (timestamp) in duplicateChecker:
                print(timestamp)
                continue
            duplicateChecker.add((timestamp))

            # Check for negetive numbers
            price = float(price)
            if price < 0:
                continue

            cleaned_data.append({'Timestamp': timestamp, 'Price': price, 'Size': size})

        self.all_data = cleaned_data

        # Print how many rows remain after the checks
        print(f"Cleaned data contains {len(self.all_data)} rows.")

    def aggregate_ohlcv(self, interval):
        ohlcv = []
        interval_seconds = self._parse_interval(interval)
        current_interval_start = None
        current_interval_data = []

        # Sort data by timestamp 
        self.all_data.sort(key=lambda row: row['Timestamp'])

        for row in self.all_data:
            dt = datetime.strptime(row['Timestamp'], '%Y-%m-%d %H:%M:%S.%f')
            price = float(row['Price'])
            size = int(row['Size'])

            # Initialize the first interval
            if current_interval_start is None:
                current_interval_start = dt
                current_interval_data.append(row)
                continue

            # If the current row belongs to the next interval than we will add the current data to OHLCV and move on
            if (dt - current_interval_start).total_seconds() >= interval_seconds:
                # Process and append the current interval's OHLCV data
                ohlcv.append(self._process_interval_data(current_interval_data))

                # Reset for the next interval
                current_interval_start = dt
                current_interval_data = [row]
            else:
                # Keep adding data to the current interval
                current_interval_data.append(row)

        # Process any remaining data for the final interval
        if current_interval_data:
            ohlcv.append(self._process_interval_data(current_interval_data))

        # Output OHLCV data to CSV
        output_file = '1h30mintervaloutput.csv'
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ohlcv)

        print(f"OHLCV data written to {output_file}.")

        print(f"Generated {len(ohlcv)} OHLCV bars.")

    def _parse_interval(self, interval_str):
        time_map = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
        total_seconds = 0
        num = ''
        for char in interval_str:
            if char.isdigit():
                num += char
            else:
                total_seconds += int(num) * time_map[char]
                num = ''
        return total_seconds

    def _process_interval_data(self, data):
        # Puts the OHCLV data in the correct format
        open_price = float(data[0]['Price'])
        close_price = float(data[-1]['Price'])
        high_price = max(float(row['Price']) for row in data)
        low_price = min(float(row['Price']) for row in data)
        volume = sum(int(row['Size']) for row in data)
        timestamp = data[0]['Timestamp']
        return {'Timestamp': timestamp, 'Open': open_price, 'High': high_price, 'Low': low_price, 'Close': close_price, 'Volume': volume}


if __name__ == "__main__":
    processor = DataProcessor(data_dir="data")
    
    # Load the CSV files
    processor.load_csv_files()
    # These are instance methods
    # Clean the data
    processor.clean_data()

    # Calling the function to format and make the OHLCV output
#   processor.aggregate_ohlcv('4s')
#   processor.aggregate_ohlcv('15m')
#   processor.aggregate_ohlcv('2h')
#   processor.aggregate_ohlcv('1d')
    processor.aggregate_ohlcv('1h30m')
