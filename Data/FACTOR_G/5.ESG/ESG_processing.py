import pandas as pd
import os

# ESG 등급을 이진 분류로 변환하는 함수
def grade_to_binary(grade):
    high_grades = ['A+', 'A', 'B+']
    return 1 if grade in high_grades else 0

# 데이터 로드
df = pd.read_csv('./Data/FACTOR_G/5.ESG/ESG평가등급.csv', dtype={'stockcode': str})

# 연도별로 처리
years = range(2019, 2025)  # 2019-2024

for year in years:
    # 해당 연도 데이터 필터링
    year_data = df[df['평가년도'] == year].copy()
    if not year_data.empty:
        # ESG 등급들을 이진값으로 변환 (1: B+ 이상, 0: B 이하)
        year_data['ESG등급_binary'] = year_data['ESG등급'].apply(grade_to_binary)
        year_data['환경_binary'] = year_data['환경'].apply(grade_to_binary)
        year_data['사회_binary'] = year_data['사회'].apply(grade_to_binary)
        year_data['지배구조_binary'] = year_data['지배구조'].apply(grade_to_binary)
        
        # 원래 등급 컬럼도 유지하고 binary 컬럼을 맨 뒤로 보내기
        columns = ['stockcode', 'ESG등급', '환경', '사회', '지배구조',
                  'ESG등급_binary', '환경_binary', '사회_binary', '지배구조_binary']
        
        # 저장
        output_path = f'./Data/FACTOR_G/5.ESG/ESG_{year}.csv'
        year_data[columns].to_csv(output_path, index=False)
        print(f"{year}년 데이터 처리 완료: {len(year_data)}개 기업")
