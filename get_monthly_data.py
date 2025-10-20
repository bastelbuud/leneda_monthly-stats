import yaml
import requests
import argparse
from datetime import datetime, timedelta
from calendar import monthrange
from typing import Dict, List, Optional, Tuple
import json
import sys
import pandas as pd
import os
import sqlite3


class MonthlyEnergyDataFetcher:
    """Fetches monthly energy metering data from Leneda API for consumers and producers."""

    def __init__(self, config_path: str = './configs/monthly.yaml', db_path: str = './db/energy_data.db'):
        """Initialize with configuration from YAML file."""
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.base_url = self.config['leneda']['url']
        self.api_path = self.config['leneda']['api']['meteringData']
        self.headers = {
            self.config['leneda']['energyId']['header']: 
                self.config['leneda']['energyId']['value'],
            self.config['leneda']['apiKey']['header']: 
                self.config['leneda']['apiKey']['value']
        }
        
        self.db_path = db_path
        
        # Separate OBIS codes by type
        self.consumption_codes = []
        self.production_codes = []
        
        for obis_info in self.config['obiscode']:
            obis_code = obis_info[0]
            # Extract the first digit after the colon (e.g., "1-1:1.29.0" -> "1", "1-1:2.29.0" -> "2")
            first_digit = obis_code.split(':')[1][0]
            
            if first_digit == '1':
                self.consumption_codes.append(obis_info)
            elif first_digit == '2':
                self.production_codes.append(obis_info)
        
        # Initialize database
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with required tables."""
        # Create data folder if it doesn't exist
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table 1: Raw metering data (one row per meter/obis/month)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metering_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                entity_type TEXT NOT NULL,
                entity_name TEXT NOT NULL,
                meter_id TEXT NOT NULL,
                obis_code TEXT NOT NULL,
                obis_category TEXT NOT NULL,
                obis_description TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                started_at TEXT,
                ended_at TEXT,
                calculated BOOLEAN,
                type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(year, month, meter_id, obis_code)
            )
        ''')
        
        # Table 2: Monthly summaries by OBIS code
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monthly_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                obis_code TEXT NOT NULL,
                obis_category TEXT NOT NULL,
                obis_description TEXT NOT NULL,
                total_value REAL NOT NULL,
                num_meters INTEGER NOT NULL,
                unit TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(year, month, obis_code)
            )
        ''')
        
        # Create indexes for better query performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_metering_year_month 
            ON metering_data(year, month)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_metering_obis 
            ON metering_data(obis_code)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_summary_year_month 
            ON monthly_summaries(year, month)
        ''')
        
        conn.commit()
        conn.close()
        
        print(f"Database initialized at: {self.db_path}")
    
    def save_to_database(self, df: pd.DataFrame, year: int, month: int):
        """Save data to SQLite database, replacing existing data for the same month."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Filter only rows with data
            df_with_data = df[df['data_available'] == True].copy()
            df_with_data['value'] = df_with_data['value'].round(2)
            
            if df_with_data.empty:
                print("No data to save to database.")
                return
            
            # Delete existing data for this year/month
            cursor.execute('''
                DELETE FROM metering_data 
                WHERE year = ? AND month = ?
            ''', (year, month))
            
            cursor.execute('''
                DELETE FROM monthly_summaries 
                WHERE year = ? AND month = ?
            ''', (year, month))
            
            # Insert metering data
            for _, row in df_with_data.iterrows():
                cursor.execute('''
                    INSERT INTO metering_data 
                    (year, month, entity_type, entity_name, meter_id, obis_code, 
                     obis_category, obis_description, value, unit, started_at, 
                     ended_at, calculated, type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['year'], row['month'], row['entity_type'], row['entity_name'],
                    row['meter_id'], row['obis_code'], row['obis_category'],
                    row['obis_description'], row['value'], row['unit'],
                    row['started_at'], row['ended_at'], row['calculated'], row['type']
                ))
            
            # Calculate and insert summaries
            summaries = df_with_data.groupby(['obis_code', 'obis_category', 'obis_description']).agg({
                'value': 'sum',
                'entity_name': 'count',
                'unit': 'first'
            }).reset_index()
            
            summaries.columns = ['obis_code', 'obis_category', 'obis_description', 
                                'total_value', 'num_meters', 'unit']
            
            for _, row in summaries.iterrows():
                cursor.execute('''
                    INSERT INTO monthly_summaries 
                    (year, month, obis_code, obis_category, obis_description, 
                     total_value, num_meters, unit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    year, month, row['obis_code'], row['obis_category'],
                    row['obis_description'], round(row['total_value'], 2),
                    row['num_meters'], row['unit']
                ))
            
            conn.commit()
            
            rows_inserted = len(df_with_data)
            summaries_inserted = len(summaries)
            
            print(f"\n{'='*80}")
            print(f"DATABASE UPDATE COMPLETE")
            print(f"{'='*80}")
            print(f"  Metering data records: {rows_inserted} (replaced existing data for {year}-{month:02d})")
            print(f"  Summary records: {summaries_inserted}")
            print(f"  Database: {self.db_path}")
            print(f"{'='*80}")
            
        except Exception as e:
            conn.rollback()
            print(f"Error saving to database: {e}")
            raise
        finally:
            conn.close()
    
    def calculate_month_dates(self, year: Optional[int] = None, month: Optional[int] = None) -> tuple:
        """
        Calculate start and end dates for the specified month.
        
        Args:
            year: Year (if None, uses current year)
            month: Month (if None, uses previous month)
            
        Returns:
            Tuple of (start_date, end_date, year, month) as strings and integers
        """
        today = datetime.now()
        
        # If no month provided, use previous month
        if month is None:
            # Go back one month
            first_day_current_month = today.replace(day=1)
            last_day_previous_month = first_day_current_month - timedelta(days=1)
            month = last_day_previous_month.month
            # If previous month is December, adjust year
            if month == 12 and year is None:
                year = today.year - 1
        
        # If no year provided, use current year (unless already adjusted for December)
        if year is None:
            year = today.year
        
        # Calculate first and last day of the month
        first_day = 1
        last_day = monthrange(year, month)[1]
        
        start_date = f"{year:04d}-{month:02d}-{first_day:02d}"
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
        
        return start_date, end_date, year, month
    
    def fetch_metering_data(self, 
                           meter_id: str, 
                           obis_code: str,
                           start_date: str,
                           end_date: str) -> Optional[Dict]:
        """
        Fetch metering data for a specific smart meter and OBIS code.
        
        Args:
            meter_id: Smart meter ID
            obis_code: OBIS code for the measurement type
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            
        Returns:
            JSON response from API or None if request fails
        """
        url = (f"{self.base_url}{self.api_path}{meter_id}/time-series/aggregated?"
               f"obisCode={obis_code}&startDate={start_date}&endDate={end_date}"
               f"&aggregationLevel=Infinite&transformationMode=Accumulation")
        
        print(f"  Fetching: {obis_code}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 404:
                print(f"    ⚠ No data available for this OBIS code")
                return None
            elif response.status_code != 200:
                print(f"    ✗ Error {response.status_code}: {response.text}")
                return None
            
            data = response.json()
            
            # Check if data exists
            if data.get('aggregatedTimeSeries') and len(data['aggregatedTimeSeries']) > 0:
                value = data['aggregatedTimeSeries'][0].get('value', 0)
                unit = data.get('unit', '')
                print(f"    ✓ Value: {value} {unit}")
                return data
            else:
                print(f"    ⚠ No time series data returned")
                return None
                
        except requests.exceptions.Timeout:
            print(f"    ✗ Request timeout")
            return None
        except requests.exceptions.RequestException as e:
            print(f"    ✗ Request error: {e}")
            return None
        except json.JSONDecodeError:
            print(f"    ✗ Invalid JSON response")
            return None
    
    def fetch_all_data(self, year: Optional[int] = None, month: Optional[int] = None) -> Tuple[Dict, pd.DataFrame]:
        """
        Fetch data for all consumers and producers for the specified month.
        
        Args:
            year: Year (if None, uses current year)
            month: Month (if None, uses previous month)
            
        Returns:
            Tuple of (results_dict, dataframe)
        """
        # Calculate dates
        start_date, end_date, calc_year, calc_month = self.calculate_month_dates(year, month)
        
        print("="*80)
        print(f"Fetching Energy Data for {calc_year}-{calc_month:02d}")
        print(f"Period: {start_date} to {end_date}")
        print("="*80)
        
        results = {
            'period': {
                'year': calc_year,
                'month': calc_month,
                'start_date': start_date,
                'end_date': end_date
            },
            'consumers': {},
            'producers': {}
        }
        
        # List to collect data for DataFrame
        df_data = []
        
        # Process CONSUMERS with CONSUMPTION codes
        print(f"\n{'='*80}")
        print("PROCESSING CONSUMERS")
        print(f"{'='*80}")
        
        consumer_names = self.config['consumers']['names']
        consumer_meters = self.config['consumers']['smartmeters']
        
        for name, meter_id in zip(consumer_names, consumer_meters):
            print(f"\n{'─'*80}")
            print(f"Consumer: {name}")
            print(f"Meter ID: {meter_id}")
            print(f"{'─'*80}")
            
            results['consumers'][name] = {
                'meter_id': meter_id,
                'data': {}
            }
            
            # Iterate through CONSUMPTION OBIS codes only
            for obis_info in self.consumption_codes:
                obis_code = obis_info[0]
                obis_category = obis_info[1]
                obis_description = obis_info[2]
                
                data = self.fetch_metering_data(meter_id, obis_code, start_date, end_date)
                
                # Prepare row for DataFrame
                row = {
                    'year': calc_year,
                    'month': calc_month,
                    'start_date': start_date,
                    'end_date': end_date,
                    'entity_type': 'consumer',
                    'entity_name': name,
                    'meter_id': meter_id,
                    'obis_code': obis_code,
                    'obis_category': obis_category,
                    'obis_description': obis_description,
                    'value': None,
                    'unit': None,
                    'started_at': None,
                    'ended_at': None,
                    'calculated': None,
                    'type': None,
                    'data_available': False
                }
                
                if data:
                    results['consumers'][name]['data'][obis_code] = {
                        'category': obis_category,
                        'description': obis_description,
                        'response': data
                    }
                    
                    # Extract data for DataFrame
                    if data.get('aggregatedTimeSeries') and len(data['aggregatedTimeSeries']) > 0:
                        ts = data['aggregatedTimeSeries'][0]
                        row['value'] = ts.get('value')
                        row['started_at'] = ts.get('startedAt')
                        row['ended_at'] = ts.get('endedAt')
                        row['calculated'] = ts.get('calculated')
                        row['type'] = ts.get('type')
                        row['unit'] = data.get('unit')
                        row['data_available'] = True
                
                df_data.append(row)
        
        # Process PRODUCERS with PRODUCTION codes
        print(f"\n{'='*80}")
        print("PROCESSING PRODUCERS")
        print(f"{'='*80}")
        
        producer_names = self.config['producers']['names']
        producer_meters = self.config['producers']['smartmeters']
        
        for name, meter_id in zip(producer_names, producer_meters):
            print(f"\n{'─'*80}")
            print(f"Producer: {name}")
            print(f"Meter ID: {meter_id}")
            print(f"{'─'*80}")
            
            results['producers'][name] = {
                'meter_id': meter_id,
                'data': {}
            }
            
            # Iterate through PRODUCTION OBIS codes only
            for obis_info in self.production_codes:
                obis_code = obis_info[0]
                obis_category = obis_info[1]
                obis_description = obis_info[2]
                
                data = self.fetch_metering_data(meter_id, obis_code, start_date, end_date)
                
                # Prepare row for DataFrame
                row = {
                    'year': calc_year,
                    'month': calc_month,
                    'start_date': start_date,
                    'end_date': end_date,
                    'entity_type': 'producer',
                    'entity_name': name,
                    'meter_id': meter_id,
                    'obis_code': obis_code,
                    'obis_category': obis_category,
                    'obis_description': obis_description,
                    'value': None,
                    'unit': None,
                    'started_at': None,
                    'ended_at': None,
                    'calculated': None,
                    'type': None,
                    'data_available': False
                }
                
                if data:
                    results['producers'][name]['data'][obis_code] = {
                        'category': obis_category,
                        'description': obis_description,
                        'response': data
                    }
                    
                    # Extract data for DataFrame
                    if data.get('aggregatedTimeSeries') and len(data['aggregatedTimeSeries']) > 0:
                        ts = data['aggregatedTimeSeries'][0]
                        row['value'] = ts.get('value')
                        row['started_at'] = ts.get('startedAt')
                        row['ended_at'] = ts.get('endedAt')
                        row['calculated'] = ts.get('calculated')
                        row['type'] = ts.get('type')
                        row['unit'] = data.get('unit')
                        row['data_available'] = True
                
                df_data.append(row)
        
        # Create DataFrame
        df = pd.DataFrame(df_data)
        
        return results, df
    
    def create_wide_format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the long format DataFrame to wide format with consumption and production columns.
        
        Args:
            df: DataFrame in long format
            
        Returns:
            DataFrame in wide format with one row per metering point
        """
        if df.empty or not df['data_available'].any():
            return pd.DataFrame()
        
        # Filter only rows with data and round values to 2 decimals
        df_filtered = df[df['data_available'] == True].copy()
        df_filtered['value'] = df_filtered['value'].round(2)
        
        # Create a mapping of OBIS codes to descriptions
        obis_descriptions = {}
        for obis_info in self.config['obiscode']:
            obis_code = obis_info[0]
            description = obis_info[2]  # Last element in the array
            obis_descriptions[obis_code] = description
        
        # Create pivot table
        pivot_data = []
        
        # Get unique metering points
        for (entity_type, entity_name, meter_id), group in df_filtered.groupby(['entity_type', 'entity_name', 'meter_id']):
            row = {
                'entity_type': entity_type,
                'name': entity_name,
                'metering_point': meter_id,
                'year': group['year'].iloc[0],
                'month': group['month'].iloc[0]
            }
            
            # Add OBIS code values as columns
            for _, record in group.iterrows():
                obis_code = record['obis_code']
                value = record['value']
                row[obis_code] = value
            
            pivot_data.append(row)
        
        # Create DataFrame
        wide_df = pd.DataFrame(pivot_data)
        
        # Reorder columns: year, month, name, metering_point, then consumption codes, then production codes
        base_cols = ['year', 'month', 'entity_type', 'name', 'metering_point']
        
        # Get consumption and production column names (OBIS codes)
        consumption_cols = [col for col in wide_df.columns if col.startswith('1-') and ':1.' in col]
        production_cols = [col for col in wide_df.columns if col.startswith('1-') and ':2.' in col]
        
        # Sort OBIS codes for consistent ordering
        consumption_cols.sort()
        production_cols.sort()
        
        # Combine all columns in desired order
        ordered_cols = base_cols + consumption_cols + production_cols
        
        # Reorder and fill NaN with 0 for missing OBIS codes
        wide_df = wide_df.reindex(columns=ordered_cols, fill_value=0)
        
        # Add totals row
        totals_row = {'year': 'TOTAL', 'month': '', 'entity_type': '', 'name': '', 'metering_point': ''}
        
        for col in consumption_cols + production_cols:
            if col in wide_df.columns:
                totals_row[col] = round(wide_df[col].sum(), 2)
        
        # Append totals row
        totals_df = pd.DataFrame([totals_row])
        wide_df = pd.concat([wide_df, totals_df], ignore_index=True)
        
        # Add description row
        description_row = {'year': 'DESCRIPTION', 'month': '', 'entity_type': '', 'name': '', 'metering_point': ''}
        
        for col in consumption_cols + production_cols:
            if col in obis_descriptions:
                description_row[col] = obis_descriptions[col]
            else:
                description_row[col] = ''
        
        # Append description row
        description_df = pd.DataFrame([description_row])
        wide_df = pd.concat([wide_df, description_df], ignore_index=True)
        
        return wide_df
    
    def save_results(self, results: Dict, df: pd.DataFrame, output_file: Optional[str] = None):
        """Save results to JSON, CSV, and Excel files in the /data folder."""
        # Create /data folder if it doesn't exist
        data_folder = 'data'
        os.makedirs(data_folder, exist_ok=True)
        
        if output_file is None:
            year = results['period']['year']
            month = results['period']['month']
            json_file = os.path.join(data_folder, f"energy_data_{year}_{month:02d}.json")
            csv_file = os.path.join(data_folder, f"energy_data_{year}_{month:02d}.csv")
            excel_file = os.path.join(data_folder, f"energy_data_{year}_{month:02d}.xlsx")
        else:
            # Remove extension and add appropriate ones
            base_name = os.path.basename(output_file).rsplit('.', 1)[0]
            json_file = os.path.join(data_folder, f"{base_name}.json")
            csv_file = os.path.join(data_folder, f"{base_name}.csv")
            excel_file = os.path.join(data_folder, f"{base_name}.xlsx")
        
        # Save JSON (original format)
        with open(json_file, 'w') as f:
            json.dump(results, indent=2, fp=f)
        
        # Create wide format DataFrame
        wide_df = self.create_wide_format_dataframe(df)
        
        # Save CSV in wide format
        if not wide_df.empty:
            wide_df.to_csv(csv_file, index=False)
            csv_saved = True
        else:
            csv_saved = False
        
        # Save Excel with multiple sheets
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main sheet - wide format with totals
            if not wide_df.empty:
                wide_df.to_excel(writer, sheet_name='Energy Data', index=False)
            
            # Original detailed data (for reference)
            df_with_data = df[df['data_available'] == True].copy()
            if not df_with_data.empty:
                df_with_data['value'] = df_with_data['value'].round(2)
                display_cols = ['year', 'month', 'entity_type', 'entity_name', 'meter_id', 
                               'obis_code', 'obis_category', 'obis_description', 'value', 'unit']
                df_with_data[display_cols].to_excel(writer, sheet_name='Detailed Data', index=False)
        
        print(f"\n{'='*80}")
        print(f"Results saved to {data_folder}/ folder:")
        print(f"  - JSON:  {os.path.basename(json_file)}")
        if csv_saved:
            print(f"  - CSV:   {os.path.basename(csv_file)} (wide format with totals)")
        print(f"  - Excel: {os.path.basename(excel_file)}")
        print(f"    Sheets: Energy Data (wide format), Detailed Data")
        print(f"{'='*80}")
    
    def print_summary(self, results: Dict, df: pd.DataFrame):
        """Print a summary of the results."""
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        
        print(f"\nTotal API calls: {df.shape[0]}")
        print(f"Successful retrievals: {df['data_available'].sum()}")
        print(f"Failed retrievals: {(~df['data_available']).sum()}")
        
        # Summary by entity type
        print(f"\n{'─'*80}")
        print("By Entity Type:")
        df_with_data = df[df['data_available'] == True].copy()
        if not df_with_data.empty:
            df_with_data['value'] = df_with_data['value'].round(2)
            entity_summary = df_with_data.groupby('entity_type').agg({
                'entity_name': 'nunique',
                'value': 'sum'
            })
            entity_summary.columns = ['Number of Entities', 'Total Value (kWh)']
            print(entity_summary.to_string())
        
        # Create and display wide format preview
        wide_df = self.create_wide_format_dataframe(df)
        
        if not wide_df.empty:
            print(f"\n{'─'*80}")
            print("WIDE FORMAT PREVIEW (with totals and descriptions):")
            print(f"{'─'*80}")
            
            # Display the wide format DataFrame
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 30)
            
            print(wide_df.to_string(index=False))
            
            print(f"\n{'─'*80}")
            print(f"Wide format: {wide_df.shape[0]-2} metering points + 1 totals row + 1 description row")
            print(f"Columns: {wide_df.shape[1]} ({len([c for c in wide_df.columns if c.startswith('1-')])} OBIS codes)")
        else:
            print("\nNo data available to display.")


def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description='Fetch monthly energy data from Leneda API for consumers and producers'
    )
    parser.add_argument(
        '--year', '-y',
        type=int,
        help='Year (default: current year, or previous year if month is December)'
    )
    parser.add_argument(
        '--month', '-m',
        type=int,
        choices=range(1, 13),
        help='Month (1-12) (default: previous month)'
    )
    parser.add_argument(
        '--config', '-c',
        default='./configs/monthly.yaml',
        help='Path to configuration YAML file (default: ./configs/monthly.yaml)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output file path (default: ./data/energy_data_YYYY_MM.*)'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save results to file'
    )
    parser.add_argument(
        '--db-path',
        default='./data/energy_data.db',
        help='Path to SQLite database (default: data/energy_data.db)'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize fetcher
        fetcher = MonthlyEnergyDataFetcher(args.config, args.db_path)
        
        print(f"\nFound {len(fetcher.consumption_codes)} consumption OBIS codes")
        print(f"Found {len(fetcher.production_codes)} production OBIS codes")
        
        # Fetch all data
        results, df = fetcher.fetch_all_data(year=args.year, month=args.month)
        
        # Extract year and month from results
        calc_year = results['period']['year']
        calc_month = results['period']['month']
        
        # Print summary
        fetcher.print_summary(results, df)
        
        # Save to database
        fetcher.save_to_database(df, calc_year, calc_month)
        
        # Save results
        if not args.no_save:
            fetcher.save_results(results, df, args.output)
        
        return df  # Return DataFrame for programmatic use
        
    except FileNotFoundError as e:
        print(f"Error: Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()