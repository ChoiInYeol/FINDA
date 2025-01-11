import pandas as pd

def process_file(year):
    # 데이터 로드
    df = pd.read_csv(f'fundamentals_{year}.csv')
    
    # stockcode 처리
    df['stockcode'] = 'A' + df['stockcode'].astype(str).str.zfill(6)
    
    # 컬럼 순서 재정렬
    columns = ['stockcode', 'Name', 'Market', 'Year', 'BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS', '날짜', '년도']
    df = df[columns]
    
    # 결과 저장
    df.to_csv(f'fundamentals_{year}_modified.csv', index=False)
    print(f"{year}년도 처리 완료")

# 2019-2024년 데이터 처리
for year in range(2019, 2025):
    try:
        process_file(year)
    except FileNotFoundError:
        print(f"{year}년도 파일이 존재하지 않습니다.")
