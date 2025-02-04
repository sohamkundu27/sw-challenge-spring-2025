import os
import csv
import threading
from datetime import datetime, timedelta
from queue import Queue
import statistics

# These are the constants for trading hours
TRADING_START = datetime.strptime("09:30", "%H:%M").time()
TRADING_END = datetime.strptime("16:00", "%H:%M").time()

# Queue to store loaded data from CSV files
data_queue = Queue()

# Class to handle all the functions
class DataProcessor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.all_data = []

    def load_csv_files(self):
        """Loads trade data from CSV files in the specified directory using multithreading."""
        files = [f for f in os.listdir(self.data_dir)]

        def worker(file):
            """Thread worker function to load CSV data into a queue."""
            file_path = os.path.join(self.data_dir, file)
            try:
                with open(file_path, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        data_queue.put(row)  # Add each row of trade data to the queue
            except Exception as e:
                print(f"Error loading {file}: {e}")

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
        """Cleans data by removing missing values, duplicates, outliers, and invalid timestamps."""
        cleaned_data = []
        duplicate_checker = set()
        price_samples = []  # Stores prices to compute median price dynamically

        while not data_queue.empty():
            row = data_queue.get()

            # Extract individual data fields
            timestamp = row['Timestamp']
            price = row['Price']
            size = row['Size']

            # Skip rows with missing values
            if not timestamp or not price or not size:
                continue

            try:
                # Parse timestamp with milliseconds
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')

                # Filter out trades outside regular trading hours
                if not (TRADING_START <= dt.time() <= TRADING_END):
                    continue
            except ValueError:
                print(f"Invalid timestamp format: {timestamp}")
                continue

            # Convert price and size to numeric values
            price = float(price)
            size = int(size)

            # Skip negative price or size values
            if price < 0 or size < 0:
                continue

            # Check for duplicates (same timestamp)
            if timestamp in duplicate_checker:
                continue
            duplicate_checker.add(timestamp)

            # Collect samples of valid prices for outlier detection
            if len(price_samples) >= 50:
                median_price = statistics.median(price_samples[-50:])  # Use last 50 prices
                if price < 0.1 * median_price:  # Price is an extreme outlier (10% of median)
                    print(f"Removed outlier: {timestamp}, Price: {price}, Size: {size}")
                    continue

            price_samples.append(price)

            cleaned_data.append({'Timestamp': timestamp, 'Price': price, 'Size': size})

        self.all_data = cleaned_data

        print(f"Cleaned data contains {len(self.all_data)} rows.")

    def aggregate_ohlcv(self, interval):
        """Aggregates cleaned trade data into OHLCV format based on a given time interval."""
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

            # If the current row belongs to the next interval, finalize current OHLCV data
            if (dt - current_interval_start).total_seconds() >= interval_seconds:
                ohlcv.append(self._process_interval_data(current_interval_data))
                current_interval_start = dt  # Start new interval
                current_interval_data = [row]
            else:
                current_interval_data.append(row)

        # Process any remaining data for the final interval
        if current_interval_data:
            ohlcv.append(self._process_interval_data(current_interval_data))

        # Output OHLCV data to CSV
        output_file = '15mintervaloutput.csv'
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ohlcv)

        print(f"OHLCV data written to {output_file}.")
        print(f"Generated {len(ohlcv)} OHLCV bars.")

    def _parse_interval(self, interval_str):
        """Parses time intervals into seconds (e.g., '1h30m' -> 5400 seconds)."""
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
        """Processes and formats OHLCV data from a list of trades within a time interval."""
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

    # Clean the data (removes missing values, duplicates, and incorrect prices)
    processor.clean_data()

    # Aggregate OHLCV data
    processor.aggregate_ohlcv('15m')
