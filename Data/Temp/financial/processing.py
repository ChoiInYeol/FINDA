import pandas as pd
import os

name = ['asset', 'debt', 'intangible', 'OCF']
years = [2019, 2020, 2021, 2022, 2023]

# 각 지표별 데이터 로드 및 전처리
dfs = {}  # 각 지표의 데이터프레임을 저장할 딕셔너리

for indicator in name:
    # 데이터 로드
    df = pd.read_csv(f'./Data/Temp/financial/{indicator}.csv')
    
    # stockcode의 A 제거
    df['stockcode'] = df['stockcode'].str.replace('A', '')
    
    # 필요한 컬럼만 선택 (stockcode와 연도 컬럼들)
    df = df[['stockcode'] + [str(year) for year in years]]
    
    # NaN값을 0으로 채우기
    df = df.fillna('NaN')
    
    dfs[indicator] = df

# 연도별로 통합
for year in years:
    year_data = None
    
    for indicator in name:
        # 해당 연도 데이터 추출
        temp_df = dfs[indicator][['stockcode', str(year)]].copy()
        temp_df.columns = ['stockcode', f'{indicator}_{year}']
        
        if year_data is None:
            year_data = temp_df
        else:
            year_data = pd.merge(year_data, temp_df, on='stockcode', how='outer')
    
    # NaN값을 0으로 채우기
    year_data = year_data.fillna('NaN')
    
    # 저장
    os.makedirs('./Data/Temp/financial/merged', exist_ok=True)
    output_path = f'./Data/Temp/financial/merged/financial_{year}.csv'
    year_data.to_csv(output_path, index=False)
    print(f"{year}년 통합 데이터 저장 완료: {len(year_data)}개 기업")