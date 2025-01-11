import pandas as pd
import os
import glob

print("1. 메타데이터 로드 시작")
# 메타데이터 로드 (모든 컬럼을 문자열로 읽기)
metadata = pd.read_csv('../Data/metadata.csv', dtype=str)
print("메타데이터 shape:", metadata.shape)
print("메타데이터 columns:", metadata.columns.tolist())

# Data/dividend 폴더 내의 모든 xlsx 파일 로드
dividend_files = glob.glob('ValueUp/3.dividend/temp/*.xlsx')
print(f"\n2. 배당 파일 목록 ({len(dividend_files)}개):", dividend_files)

# 각 파일에서 데이터 읽기
for file in dividend_files:
    print(f"\n3. 처리 중인 파일: {file}")
    
    # 파일명에서 연도 추출
    year = os.path.basename(file).split('_')[1]
    print(f"추출된 연도: {year}")
    
    # 엑셀 파일 읽기
    print("엑셀 파일 로드 중...")
    df = pd.read_excel(file, dtype=str)
    print(f"로드된 데이터 shape: {df.shape}")
    print(f"원본 columns: {df.columns.tolist()}")
    
    # 기업명 칼럼 제거
    df = df.drop(columns=['기업명'])
    
    # 기업코드에 'A' 접두어 추가
    print("기업코드 처리 중...")
    df['기업코드'] = df['기업코드'].apply(lambda x: f'A{x}')
    print("기업코드 샘플:", df['기업코드'].head().tolist())
    
    # 기업 코드 순으로 정렬
    df = df.sort_values('기업코드')
    
    # 메타데이터랑 비교해서 존재하는 것만 추리기
    print("\n4. 메타데이터와 매칭 전 기업 수:", len(df))
    df = df[df['기업코드'].isin(metadata['stockcode'])]
    print("메타데이터와 매칭 후 기업 수:", len(df))
    
    # 메타데이터랑 칼럼이랑 연결
    print("\n5. 메타데이터 병합 중...")
    df = pd.merge(df, metadata, left_on='기업코드', right_on='stockcode', how='inner')
    print(f"병합 후 데이터 shape: {df.shape}")
    print(f"병합 후 columns: {df.columns.tolist()}")
    
    # 연도 설정
    year_prev = int(year) - 1
    year_now = int(year)
    
    try:
        # 칼럼 순서 정리
        print("\n6. 최종 칼럼 정리 중...")
        df = df[['stockcode', 'Name', 'Market', 'gics', 'avg_marcap', 'avg_amount',
                 f'{year_prev}_배당', f'{year_prev}_자사주', 
                 f'{year_now}_배당', f'{year_now}_자사주', 
                 f'최근2년_활동']]
        print("칼럼 정리 완료")
    except KeyError as e:
        print(f"오류 발생! 없는 칼럼: {e}")
        print(f"사용 가능한 칼럼들: {df.columns.tolist()}")
        continue

    # NaN 값 처리
    df = df.fillna('NaN')
    
    # 연도별로 데이터 저장
    output_path = f'../dividend_{year}.csv'
    print(f"\n7. 저장 중... : {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"저장 완료: {output_path}")
    print("-" * 80)