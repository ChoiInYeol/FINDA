import pandas as pd
import numpy as np
from datetime import datetime
from marcap import marcap_data
import os

def get_stock_type(name, code):
    """주식 종류를 판별하는 함수"""
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

def process_market_data():
    # 연도별 처리
    years = range(2018, 2025)
    for year in years:
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        # 해당 연도의 시가총액 데이터 로드
        df = marcap_data(start_date, end_date)
        
        # 주식 유형 추가
        df['StockType'] = df.apply(lambda x: get_stock_type(x['Name'], x['Code']), axis=1)
        
        # 일반주만 필터링
        df = df[df['StockType'] == 'Common']
        
        # 종목별 일평균 시가총액, 거래대금 계산
        stock_summary = df.groupby('Code').agg({
            'Marcap': 'mean',      # 일평균 시가총액
            'Amount': 'mean',      # 일평균 거래대금
            'Name': 'first',       # 종목명
            'Market': 'first',     # 시장구분
            'Stocks': 'last'       # 상장주식수
        }).reset_index()
        
        # 컬럼명 변경 및 종목코드 앞에 'A' 추가
        stock_summary = stock_summary.rename(columns={
            'Code': 'stockcode',
            'Marcap': 'avg_marcap',
            'Amount': 'avg_amount'
        })
        stock_summary['stockcode'] = 'A' + stock_summary['stockcode']
        
        # 일평균 시가총액 순으로 정렬
        stock_summary = stock_summary.sort_values(by='avg_marcap', ascending=False)
        
        # KONEX 제외
        stock_summary = stock_summary[stock_summary['Market'] != 'KONEX']
        
        # 저장
        os.makedirs('./Data/ValueUp', exist_ok=True)
        output_path = f'./Data/ValueUp/marketcap/cap_{year}.csv'
        stock_summary.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"\n{year}년 처리 완료:")
        print(f"- 전체 종목 수: {len(stock_summary)}개")
        print(f"- 평균 시가총액: {stock_summary['avg_marcap'].mean()/1e8:.1f}억원")
        print(f"- 평균 거래대금: {stock_summary['avg_amount'].mean()/1e8:.1f}억원")
        print(f"- KOSPI 종목 수: {len(stock_summary[stock_summary['Market']=='KOSPI'])}개")
        print(f"- KOSDAQ 종목 수: {len(stock_summary[stock_summary['Market']=='KOSDAQ'])}개")

if __name__ == "__main__":
    process_market_data()