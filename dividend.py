import pandas as pd
import os
import glob

# 메타데이터 로드 (모든 컬럼을 문자열로 읽기)
metadata = pd.read_csv('Data/metadata.csv', dtype=str)

# Data/dividend 폴더 내의 모든 xlsx 파일 로드
dividend_files = glob.glob('Data/dividend/*.xlsx')

# 2018_2019_배당_자사주_활동기업
# 2019_2020_배당_자사주_활동기업
# 2020_2021_배당_자사주_활동기업
# 2021_2022_배당_자사주_활동기업
# 2022_2023_배당_자사주_활동기업
# 2023_2024_배당_자사주_활동기업

# 2년씩 묶여있음
# 2년 연속 배당 또는 자사주 매입을 했는가를 판별하는 Bool 데이터 존재

# xlsx 파일을 csv 파일로 저장해야 함

# 각 파일에서 데이터 읽기
for file in dividend_files:
    # 파일명에서 연도 추출
    year = os.path.basename(file).split('_')[1]
    
    # 엑셀 파일 읽기
    df = pd.read_excel(file, dtype=str)
    
    # 기업명 칼럼 제거
    df = df.drop(columns=['기업명'])

    # 기업 코드 순으로 정렬
    df = df.sort_values('기업코드')
    
    # 메타데이터랑 비교해서 존재하는 것만 추리기
    df = df[df['기업코드'].isin(metadata['stockcode'])]
    
    # 메타데이터랑 칼럼이랑 연결
    df = pd.merge(df, metadata, left_on='기업코드', right_on='stockcode', how='inner')
    
    # 칼럼 정리
    # stockcode,dartcode,name,market,gics, 나머지는 배당 관련 칼럼
    year_prev = int(year) - 1
    year_now = int(year)
    
    print(df.head())
    
    # 칼럼 순서 정리
    df = df[['stockcode', 'dartcode', 'name', 'market', 'gics', f'{year_prev}_배당', f'{year_prev}_자사주', f'{year_now}_배당', f'{year_now}_자사주', f'최근2년_활동']]
    
    # NaN 값 NaN으로 채우기
    df = df.fillna('NaN')
    
    # 연도별로 데이터 저장
    df.to_csv(f'Data/dividend/dividend_{year}.csv', index=False, encoding='utf-8-sig')

print(df.head())