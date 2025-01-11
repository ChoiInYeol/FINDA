from pykrx import stock as pkstock
import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm

def get_yearly_fundamentals_by_ticker(year, ticker):
    """기존 코드와 동일"""
    try:
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        df = pkstock.get_market_fundamental(start_date, end_date, ticker, freq="y")
        
        if not df.empty:
            df['Year'] = year
            df['stockcode'] = ticker
            return df
    except Exception as e:
        print(f"\nError processing ticker {ticker} for year {year}: {str(e)}")
    return None

def process_market_data(start_year, end_year, market_type):
    """�도별로 데이터를 처리하고 개별 저장합니다."""
    for year in tqdm(range(start_year, end_year + 1), desc=f"처리 연도"):
        print(f"\n{year}년 {market_type} 데이터 처리 중...")
        cap_file = f"Data/ValueUp/marketcap/cap_{year}.csv"
        
        if not os.path.exists(cap_file):
            print(f"Warning: {cap_file} not found")
            continue
            
        cap_df = pd.read_csv(cap_file)
        tickers = cap_df[cap_df['Market'] == market_type]['stockcode'].unique()
        
        year_data = []
        for ticker in tqdm(tickers, desc=f"{year}년 종목 처리", leave=False):
            ticker = str(ticker).replace('A', '').zfill(6)
            result = get_yearly_fundamentals_by_ticker(year, ticker)
            if result is not None:
                stock_info = cap_df[cap_df['stockcode'] == f'A{ticker}'].iloc[0]
                result['Name'] = stock_info['Name']
                result['Market'] = stock_info['Market']
                year_data.append(result)
        
        if year_data:
            yearly_df = pd.concat(year_data)
            output_file = f'Data/ValueUp/fundamentals/{market_type.lower()}_fundamentals_{year}.csv'
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            yearly_df.to_csv(output_file, index=True)
            print(f"{year}년 처리 완료: {len(year_data)}개 종목")

# 메인 실행 부분
start_year = 2019
end_year = 2024

#print("\nKOSPI 데이터 처리 시작...")
#process_market_data(start_year, end_year, 'KOSPI')

print("\nKOSDAQ 데이터 처리 시작...")
process_market_data(start_year, end_year, 'KOSDAQ')