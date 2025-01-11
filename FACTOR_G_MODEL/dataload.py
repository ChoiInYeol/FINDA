import pandas as pd
import os
import glob

# 연도 설정
years = range(2019, 2024)  # 2019-2023

# 각 연도별로 처리
for year in years:
    merged_data = None
    
    # 각 폴더에서 해당 연도의 파일 찾기
    folders = {
        'financial': './Data/FACTOR_G/1.Financial',
        'growth': './Data/FACTOR_G/2.Growth',
        'ownership': './Data/FACTOR_G/3.Shareholding',
        'directors': './Data/FACTOR_G/4.Directors',
        'esg': './Data/FACTOR_G/5.ESG'
    }
    
    for category, folder in folders.items():
        file_pattern = f"{folder}/*_{year}.csv"
        files = glob.glob(file_pattern)
        
        for file in files:
            df = pd.read_csv(file, dtype={'stockcode': str, 'dartcode': str})
            
            if merged_data is None:
                merged_data = df
            else:
                merged_data = pd.merge(merged_data, df, on='stockcode', how='outer')
    
    if merged_data is not None:
        # 컬럼 그룹 정의
        index_cols = ['stockcode', 'dartcode', 'name']  # 기업 식별 정보
        meta_cols = ['market', 'gics']  # 기업 메타 정보
        
        # Y 라벨 컬럼
        y_cols = ['ESG등급', 'ESG등급_binary', '환경', '환경_binary', 
                 '사회', '사회_binary', '지배구조', '지배구조_binary']
        
        # 이사회 관련 컬럼
        director_cols = ['전체이사수', '사외이사수', '사외이사비율']
        
        # 지분 관련 컬럼
        ownership_cols = ['외국인투자지분율_' + str(year), '최대주주지분율_' + str(year)]
        
        # 나머지 재무/성장 특성들
        other_cols = [col for col in merged_data.columns 
                     if col not in (index_cols + meta_cols + y_cols + 
                                  director_cols + ownership_cols)]
        
        # 최종 컬럼 순서
        final_cols = (index_cols + meta_cols + director_cols + ownership_cols + 
                     other_cols + y_cols)
        
        # 컬럼 재정렬
        merged_data = merged_data[final_cols]
        
        # 저장
        os.makedirs('./Data/FACTOR_G/final', exist_ok=True)
        output_path = f'./Data/FACTOR_G/final/factors_{year}.csv'
        merged_data.to_csv(output_path, index=False)
        
        print(f"\n{year}년 데이터 처리 완료:")
        print(f"- 기업 수: {len(merged_data)}개")
        print(f"- 컬럼 구성:")
        print(f"  * 식별 정보: {len(index_cols)}개")
        print(f"  * 메타 정보: {len(meta_cols)}개")
        print(f"  * 이사회 정보: {len(director_cols)}개")
        print(f"  * 지분 정보: {len(ownership_cols)}개")
        print(f"  * 재무/성장 특성: {len(other_cols)}개")
        print(f"  * Y 라벨: {len(y_cols)}개")