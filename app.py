import pandas as pd
from yahoofinancials import YahooFinancials
from datetime import datetime, timedelta
import logging, os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

dir_path = 'C:/Users/prateek.pandey.ext/OneDrive - Everest Group/Documents/Desktop/Stock Price Index'

try:
    stocks_info = pd.read_excel(os.path.join(dir_path, 'stocks_list.xlsx')).to_dict('records')
except Exception as e:
    logging.error(f"Failed to load stocks information: {e}")

currency_pairs = {
    "USD/EURO": "EURUSD=X",
    "USD/INR": "INR=X"
}

def fetch_currency_data(start_date, end_date):
    """Fetch historical currency data for specified pairs between given dates."""
    currency_data = pd.DataFrame()
    for pair, ticker in currency_pairs.items():
        try:
            yahoo_financials = YahooFinancials(ticker)
            historical_data = yahoo_financials.get_historical_price_data(start_date, end_date, 'daily')
            if historical_data[ticker]['prices']:
                temp_df = pd.DataFrame(historical_data[ticker]['prices'])
                temp_df['Date'] = pd.to_datetime(temp_df['formatted_date'])
                temp_df = temp_df[['Date', 'adjclose']].rename(columns={'adjclose': pair})
                if not currency_data.empty:
                    currency_data = currency_data.merge(temp_df, on='Date', how='outer')
                else:
                    currency_data = temp_df
        except Exception as e:
            logging.error(f"Failed to fetch currency data for {pair}: {e}")
    
    try:
        currency_data['Date'] = pd.to_datetime(currency_data['Date']).dt.date
        currency_data = currency_data.sort_values(by='Date').ffill()
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D').date
        all_dates_df = pd.DataFrame({'Date': all_dates})
        currency_data = all_dates_df.merge(currency_data, on='Date', how='left').ffill()
        return currency_data.drop_duplicates('Date')
    except Exception as e:
        logging.error(f"Error processing currency data: {e}")
        return pd.DataFrame()

def calculate_financial_metrics(df, base_currency, currency_data):
    """Calculate financial metrics like monthly and yearly changes, YTD, etc."""
    try:
        df['Date'] = pd.to_datetime(df['Date'])
        currency_data['Date'] = pd.to_datetime(currency_data['Date'])
        
        all_dates = pd.date_range(start=currency_data['Date'].min(), end=currency_data['Date'].max(), freq='D')
        all_dates_df = pd.DataFrame({'Date': all_dates})
        
        df = all_dates_df.merge(df, on='Date', how='left')
        df = df.merge(currency_data, on='Date', how='left')
        df = df.ffill()

        for column in ['1-month', '3-month', '1-year', 'YTD', 'USD End Price', 'USD Beg. Price']:
            df[column] = None

        df.set_index('Date', inplace=True)
        df.sort_index(inplace=True)
        
        if base_currency != 'USD':
            currency_column = 'USD/INR' if base_currency == 'INR' else 'USD/EURO'
            df['USD End Price'] = df.apply(
                lambda x: round(x['End. share price'] / x[currency_column], 2) if pd.notnull(x[currency_column]) else None,
                axis=1
            )
        else:
            df['USD End Price'] = round(df['End. share price'], 2)

        df['USD Beg. Price'] = df['USD End Price'].shift(1)

        for date in df.index:
            year = date.year
            first_day_of_year = pd.Timestamp(year=year, month=1, day=1)
            if first_day_of_year in df.index:
                beginning_price = df.at[first_day_of_year, 'USD End Price']
                df.at[date, 'YTD'] = round((df.at[date, 'USD End Price'] / beginning_price) - 1, 2)

            for months_back, label in [(1, '1-month'), (3, '3-month'), (12, '1-year')]:
                lookback_date = date - pd.DateOffset(months=months_back)
                if lookback_date in df.index:
                    past_price = df.at[lookback_date, 'USD End Price']
                    current_price = df.at[date, 'USD End Price']
                    if past_price is not None and current_price is not None:
                        df.at[date, label] = round((current_price / past_price) - 1, 2)
                    else:
                        logging.warning(f"Past price or current price is None for {label} calculation on date {date}")

        df.reset_index(inplace=True)
        df.sort_values(by='Date', inplace=True)

        return df
    except Exception as e:
        logging.error(f"Error calculating financial metrics: {e}")
        return pd.DataFrame()

def generate_stock_data(start_year, filename):
    """Fetch and generate stock data starting from the specified year."""
    actual_start_date = f"{start_year}-01-01"
    historical_start_date = (pd.Timestamp(actual_start_date) - pd.DateOffset(months=13)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    final_df = pd.DataFrame()

    currency_data = fetch_currency_data(historical_start_date, end_date)
    if currency_data.empty:
        logging.error("Currency data fetching failed.")
        return

    for stock in stocks_info:
        try:
            yahoo_financials = YahooFinancials(stock['ticker'])
            historical_data = yahoo_financials.get_historical_price_data(historical_start_date, end_date, 'daily')
            if historical_data[stock['ticker']]['prices']:
                temp_df = pd.DataFrame(historical_data[stock['ticker']]['prices'])
                temp_df['End. share price'] = temp_df['adjclose']
                temp_df['Date'] = pd.to_datetime(temp_df['formatted_date'])
                temp_df['Account name'] = stock['name']
                temp_df['Currency'] = stock['currency']
                temp_df['Stock exchange / Instrument code'] = stock['exchange']
                temp_df['Ticker'] = stock['ticker']

                temp_df = calculate_financial_metrics(temp_df, stock['currency'], currency_data)
                if temp_df.empty:
                    logging.error(f"Failed to calculate financial metrics for {stock['ticker']}.")
                temp_df.drop_duplicates(subset=['Ticker', 'Date'], inplace=True)
                final_df = pd.concat([final_df, temp_df], ignore_index=True)
        except Exception as e:
            logging.error(f"Failed to fetch data for stock {stock['ticker']}: {e}")

    desired_columns = [
        'Account name', 'Currency', 'Stock exchange / Instrument code', 'Ticker', 'Date', 
        'End. share price', '1-month', '3-month', '1-year', 'YTD', 'USD/EURO', 'USD/INR', 'USD End Price', 'USD Beg. Price'
    ]

    try:
        final_df = final_df[pd.to_datetime(final_df['Date']) >= pd.Timestamp(actual_start_date)]
        final_df = final_df[desired_columns]
        final_df.to_excel(filename, index=False)
    except Exception as e:
        logging.error(f"Failed to save data to {filename}: {e}")

def calculate_and_save_average(input_file, output_file):
    """Calculate the straight average of selected columns for each date and save to a new Excel file."""
    try:
        df = pd.read_excel(input_file)

        grouped_df = df.groupby('Date').mean(numeric_only=True).reset_index()

        grouped_df = grouped_df.round(2)

        averages_df = grouped_df[['Date', '1-month', '3-month', '1-year', 'YTD']].rename(columns={
            '1-month': '1-Month',
            '3-month': '3-Month',
            '1-year': '1-Year',
            'YTD': 'YTD'
        })

        averages_df.to_excel(output_file, index=False)
    except Exception as e:
        logging.error(f"Failed to calculate and save averages: {e}")

try:
    generate_stock_data(2023, 'stock_and_currency_data_since_2023.xlsx')
    generate_stock_data(2014, 'stock_and_currency_data_since_2014.xlsx')
    calculate_and_save_average('stock_and_currency_data_since_2023.xlsx', 'average_stock_metrics.xlsx')
    logging.info("Script executed successfully.")
except Exception as e:
    logging.error(f"Automation run failed: {e}")
