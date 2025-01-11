import pandas as pd
import numpy as np
import os

def first_screening(year):
    """
    1차 선정: 시장대표성 기준과 유동성 기준 적용
    - 시장대표성: 일평균시가총액 상위 400위 이내 (기존 종목은 440위 이내)
    - 유동성: 일평균거래대금 상위 80% 이내
    """
    # 데이터 로드
    marketcap = pd.read_csv(f'../Data/ValueUp/1.marketcap/cap_{year}.csv')
    
    # stockcode를 문자열로 변환하고 6자리로 맞추기
    marketcap['stockcode'] = marketcap['stockcode'].astype(str).str.zfill(6)
    
    # 시가총액 순위 계산
    marketcap['cap_rank'] = marketcap['avg_marcap'].rank(ascending=False)
    
    # 거래대금 백분위 계산
    marketcap['amount_percentile'] = marketcap['avg_amount'].rank(pct=True)
    
    # 1차 선정 조건 적용
    condition = ((marketcap['cap_rank'] <= 400) & 
                (marketcap['amount_percentile'] >= 0.2))  # 상위 80% = 하위 20% 초과
    
    selected = marketcap[condition].copy()
    selected = selected.sort_values('avg_marcap', ascending=False)
    
    print(f"시가총액 상위 400위 & 거래대금 상위 80% 기업 수: {len(selected)}")
    return selected

def second_screening(year, first_selected):
    """
    2차 선정: 수익성 기준과 주주환원 기준 적용
    - 수익성: 최근 2개 사업연도 연속 당기순이익이 적자가 아니고, 최근 2개 사업연도 합산 당기순이익이 적자가 아닌 종목
    - 주주환원: 최근 2년 연속 배당 또는 자사주소각 실시
    """
    # 배당 데이터 로드
    dividend = pd.read_csv(f'../Data/ValueUp/3.dividend/dividend_{year}.csv')
    dividend['stockcode'] = dividend['stockcode'].astype(str).str.zfill(6)
    
    # 재무제표 데이터 로드 (현재년도)
    fundamentals = pd.read_csv(f'../Data/ValueUp/2.fundamentals/fundamentals_{year}_modified.csv')
    fundamentals['stockcode'] = fundamentals['stockcode'].astype(str).str.zfill(6)
    
    # 재무제표 데이터 로드 (전년도)
    prev_fundamentals = pd.read_csv(f'../Data/ValueUp/2.fundamentals/fundamentals_{year-1}_modified.csv')
    prev_fundamentals['stockcode'] = prev_fundamentals['stockcode'].astype(str).str.zfill(6)
    
    # 1차 선정된 종목들만 필터링
    dividend = dividend[dividend['stockcode'].isin(first_selected['stockcode'])]
    fundamentals = fundamentals[fundamentals['stockcode'].isin(first_selected['stockcode'])]
    prev_fundamentals = prev_fundamentals[prev_fundamentals['stockcode'].isin(first_selected['stockcode'])]
    
    # 배당 또는 자사주 매입 여부 확인 (True/False 형식)
    prev_year_col = f'{year-1}_배당'
    curr_year_col = f'{year}_배당'
    prev_year_buyback = f'{year-1}_자사주'
    curr_year_buyback = f'{year}_자사주'
    
    print(f"배당 데이터 컬럼: {dividend.columns.tolist()}")
    
    # True/False 형식의 데이터를 그대로 사용 (배당 또는 자사주 중 하나만 만족하면 됨)
    dividend['shareholder_return'] = ((dividend[prev_year_col]) | 
                                    (dividend[prev_year_buyback])) & \
                                   ((dividend[curr_year_col]) | 
                                    (dividend[curr_year_buyback]))
    
    # 재무데이터 병합 (현재년도 + 전년도)
    merged = pd.merge(fundamentals, prev_fundamentals, on='stockcode', suffixes=('', '_prev'))
    
    # 2개 사업연도 평균 계산
    merged['avg_PBR'] = (merged['PBR'] + merged['PBR_prev']) / 2
    merged['avg_ROE'] = ((merged['EPS'] / merged['BPS']) + (merged['EPS_prev'] / merged['BPS_prev'])) / 2
    
    # 수익성 확인 (2개년 연속 및 합산 당기순이익 양수)
    merged['profit_positive'] = (merged['EPS'] > 0) & (merged['EPS_prev'] > 0) & \
                               ((merged['EPS'] + merged['EPS_prev']) > 0)
    
    # metadata에서 GICS 정보 로드
    metadata = pd.read_csv('../Data/metadata.csv')
    metadata['stockcode'] = metadata['stockcode'].astype(str).str.zfill(6)
    
    # 재무데이터와 GICS 정보 병합 (시가총액 정보 포함)
    merged = pd.merge(merged, metadata[['stockcode', 'gics']], on='stockcode', how='left')
    merged = pd.merge(merged, first_selected[['stockcode', 'avg_marcap', 'avg_amount']], on='stockcode', how='left')
    
    # GICS 정보가 없는 종목 제외
    merged = merged.dropna(subset=['gics'])
    
    # 배당데이터와 병합
    final = pd.merge(merged, dividend[['stockcode', 'shareholder_return']], on='stockcode', how='inner')
    
    # 조건을 만족하는 종목 선택
    selected = final[final['shareholder_return'] & final['profit_positive']].copy()
    
    print(f"주주환원 기업 수: {len(dividend[dividend['shareholder_return']])}")
    print(f"수익성 있는 기업 수: {len(merged[merged['profit_positive']])}")
    print(f"두 조건 모두 만족하는 기업 수: {len(selected)}")
    
    return selected

def third_screening(year, second_selected):
    """
    3차 선정: 시장평가 기준 적용
    - 최근 2개 사업연도 평균 PBR의 순위비율이 50% 이내이거나
    - 최근 2개 사업연도 평균 PBR의 산업군 내 순위비율이 50% 이내
    - 기존 구성종목은 60% 이내면 유지
    """
    # 재무제표 데이터는 이미 2차 선정에서 평균이 계산되어 있음
    fundamentals = second_selected.copy()
    
    # PBR 순위비율 계산
    fundamentals['pbr_rank_ratio'] = fundamentals['avg_PBR'].rank(pct=True)
    
    # 산업군별 PBR 순위비율 계산
    fundamentals['industry_pbr_rank_ratio'] = fundamentals.groupby('gics')['avg_PBR'].rank(pct=True)
    
    # 조건을 만족하는 종목 선택 (전체 또는 산업군 내 PBR 순위비율이 50% 이내)
    condition = (fundamentals['pbr_rank_ratio'] <= 0.5) | \
               (fundamentals['industry_pbr_rank_ratio'] <= 0.5)
    
    # TODO: 기존 구성종목에 대해서는 60% 이내면 유지하는 로직 추가 필요
    
    selected = fundamentals[condition].copy()
    
    print(f"PBR 순위비율 50% 이내 기업 수: {len(fundamentals[fundamentals['pbr_rank_ratio'] <= 0.5])}")
    print(f"산업군 내 PBR 순위비율 50% 이내 기업 수: {len(fundamentals[fundamentals['industry_pbr_rank_ratio'] <= 0.5])}")
    print(f"선정된 기업 수: {len(selected)}")
    
    return selected

def fourth_screening(year, third_selected):
    """
    4차 선정: 자본효율성 기준 적용
    - 최근 2개 사업연도 평균 ROE 산업군 순위비율 기준 상위 100종목
    - 기존 구성종목은 120위 이내 유지
    - 신규 종목은 80위 이내여야 함
    """
    # 재무제표 데이터는 이미 2차 선정에서 평균이 계산되어 있음
    fundamentals = third_selected.copy()
    
    # 산업군별 ROE 순위 계산
    fundamentals['industry_roe_rank'] = fundamentals.groupby('gics')['avg_ROE'].rank(ascending=False)
    
    # TODO: 기존/신규 구성종목 구분하여 다른 기준 적용하는 로직 추가 필요
    
    # 상위 100종목 선정
    selected = fundamentals.nsmallest(min(100, len(fundamentals)), 'industry_roe_rank').copy()
    
    print(f"ROE 기준 선정된 기업 수: {len(selected)}")
    if len(selected) > 0:
        print("\nROE 상위 10개 기업:")
        print(selected[['stockcode', 'Name', 'Market', 'avg_ROE']].sort_values('avg_ROE', ascending=False).head(10))
    
    # 최종 결과 칼럼 정리
    selected = selected[[
        # 기본 정보
        'stockcode', 'Name', 'Market', 'gics',
        
        # 1차 스크리닝 관련 (시장대표성, 유동성)
        'avg_marcap', 'avg_amount',
        
        # 2차 스크리닝 관련 (수익성, 주주환원)
        'EPS', 'EPS_prev', 'shareholder_return',
        
        # 3차 스크리닝 관련 (시장평가)
        'avg_PBR', 'pbr_rank_ratio', 'industry_pbr_rank_ratio',
        
        # 4차 스크리닝 관련 (자본효율성)
        'avg_ROE', 'industry_roe_rank'
    ]].copy()
    
    return selected

def main():
    results = {}
    for year in range(2020, 2025):  # 2020-2023년
        print(f"\n{year}년 Value-Up 지수 구성종목 선정 시작")
        
        # 1차 선정
        first = first_screening(year)
        print(f"1차 선정 결과: {len(first)}개 기업")
        
        # 2차 선정
        second = second_screening(year, first)
        print(f"2차 선정 결과: {len(second)}개 기업")
        
        # 3차 선정
        third = third_screening(year, second)
        print(f"3차 선정 결과: {len(third)}개 기업")
        
        # 4차 선정
        fourth = fourth_screening(year, third)
        print(f"4차 선정 결과: {len(fourth)}개 기업")
        
        results[year] = fourth
        
        # 결과 저장
        fourth.to_csv(f'value_up_constituents_{year}.csv', index=False)
        
        # 선정 과정 상세 정보 출력
        if len(fourth) > 0:
            print("\n선정된 기업 목록:")
            print(fourth[['stockcode', 'Name', 'Market', 'avg_ROE', 'avg_PBR']].sort_values('avg_ROE', ascending=False))

if __name__ == "__main__":
    main() 