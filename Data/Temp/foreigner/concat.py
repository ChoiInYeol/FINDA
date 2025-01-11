import pandas as pd
import os
import glob

name = ['외국인투자지분율', '최대주주지분율']
years = range(2019, 2025)  # 2019-2023

# 연도별로 데이터 병합
for year in years:
    year_data = None
    
    # 각 지표별 데이터 로드
    for indicator in name:
        # 해당 연도의 파일 찾기
        file_path = f'./Data/FACTOR_G/foreigner/{indicator}_{year}.csv'
        
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            
            # stockcode를 문자열로 변환
            df['stockcode'] = df['stockcode'].astype(str)
            
            # 컬럼명 변경
            df.columns = ['stockcode', f'{indicator}_{year}']
            
            if year_data is None:
                year_data = df
            else:
                year_data = pd.merge(year_data, df, on='stockcode', how='outer')
        else:
            # 파일이 없는 경우 빈 DataFrame 생성
            empty_df = pd.DataFrame(columns=['stockcode', f'{indicator}_{year}'])
            if year_data is None:
                year_data = empty_df
            else:
                year_data = pd.merge(year_data, empty_df, on='stockcode', how='outer')
    
    if year_data is not None:
        # 저장
        os.makedirs('./Data/FACTOR_G/foreigner/merged', exist_ok=True)
        output_path = f'./Data/FACTOR_G/foreigner/merged/ownership_{year}.csv'
        year_data.to_csv(output_path, index=False)
        print(f"{year}년 데이터 병합 완료: {len(year_data)}개 기업")
