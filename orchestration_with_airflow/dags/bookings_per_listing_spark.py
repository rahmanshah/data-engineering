from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listings_file", required=True, help="Path to the monthly listings file")
    parser.add_argument("--bookings_file", required=True, help="Path to the hourly bookings file")
    parser.add_argument("--output_path", required=True, help="Output path for the aggregated results")
    args = parser.parse_args()


if __name__ == '__main__':
    main()