import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import time
import os

def get_stock_type(row):
    """주식 종류를 판별하는 함수"""
    name = row['Name']
    code = row['Code']
    
    # 스팩 여부 확인
    if '스팩' in name or ('제' in name and any(str(i) in name for i in range(10))):
        return 'SPAC'
    
    # 우선주 여부 확인
    if code[-1] != '0':
        return 'Preferred'
    
    # 리츠 여부 확인
    if name.endswith('리츠'):
        return 'REIT'
    
    # 나머지는 일반 주식
    return 'Common'

def get_stock_data():
    # 데이터 저장 폴더 생성
    os.makedirs('Data/KOSPI', exist_ok=True)
    os.makedirs('Data/KOSDAQ', exist_ok=True)
    
    # 코스피, 코스닥 종목 리스트 가져오기
    kospi_stocks = fdr.StockListing('KOSPI')
    kosdaq_stocks = fdr.StockListing('KOSDAQ')
    
    # ETF 제외를 위해 종목명에 'ETF'가 포함되지 않은 종목만 필터링
    kospi_stocks = kospi_stocks[~kospi_stocks['Name'].str.contains('ETF', na=False)]
    kosdaq_stocks = kosdaq_stocks[~kosdaq_stocks['Name'].str.contains('ETF', na=False)]
    
    # 시작일과 종료일 설정
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = '2007-01-01'
    
    # 일반주 카운터
    common_counter = {'KOSPI': 0, 'KOSDAQ': 0}
    
    # 코스피 종목 데이터 다운로드
    print("코스피 종목 다운로드 중...")
    for _, row in kospi_stocks.iterrows():
        try:
            # 일반주만 처리
            if get_stock_type(row) == 'Common':
                df = fdr.DataReader(row['Code'], start_date, end_date)
                file_name = f"Data/KOSPI/{row['Code']}_{row['Name']}.csv"
                df.to_csv(file_name)
                common_counter['KOSPI'] += 1
                time.sleep(0.1)
        except Exception as e:
            print(f"오류 발생 - 종목: {row['Code']}, 에러: {str(e)}")
    
    # 코스닥 종목 데이터 다운로드
    print("코스닥 종목 다운로드 중...")
    for _, row in kosdaq_stocks.iterrows():
        try:
            # 일반주만 처리
            if get_stock_type(row) == 'Common':
                df = fdr.DataReader(row['Code'], start_date, end_date)
                file_name = f"Data/KOSDAQ/{row['Code']}_{row['Name']}.csv"
                df.to_csv(file_name)
                common_counter['KOSDAQ'] += 1
                time.sleep(0.1)
        except Exception as e:
            print(f"오류 발생 - 종목: {row['Code']}, 에러: {str(e)}")
    
    # 다운로드 통계 출력
    print("\n=== 다운로드 통계 ===")
    print(f"KOSPI 일반주: {common_counter['KOSPI']}개")
    print(f"KOSDAQ 일반주: {common_counter['KOSDAQ']}개")
    print(f"총 다운로드 종목 수: {sum(common_counter.values())}개")

# 데이터 다운로드 실행
get_stock_data()