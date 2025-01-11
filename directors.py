import pandas as pd
import os
import glob

# 메타데이터 로드 (모든 컬럼을 문자열로 읽기)
metadata = pd.read_csv('Data/metadata.csv', dtype=str)

# Data/corps 폴더 내의 모든 csv 파일 로드
corps_files = glob.glob('Data/temp/corps/*.csv')

# 파일 이름 규칙을 이용해서 어떤 기업들이 있는지 데이터프레임으로 정리
# 규칙은 market_dartcode_name.csv 임
corps_info = []
for file in corps_files:
    # 파일명에서 확장자 제거
    filename = os.path.basename(file).replace('.csv', '')
    
    # 파일명을 '_'로 분리
    market, dartcode, name = filename.split('_')
    
    corps_info.append({
        'market': market,
        'dartcode': dartcode, 
        'name': name
    })

# metadata랑 비교해서 존재하는 것만 추리기
corps_df = pd.DataFrame(corps_info)
corps_df = corps_df[corps_df['dartcode'].isin(metadata['dartcode'])]
print(corps_df.head(), len(corps_df))

# 시장구분,연도,분기,기준일자,전체이사수,사외이사수,사외이사비율 이라는 칼럼을 가지고 있음
# 연도별로 각 기업들을 묶어서 데이터프레임으로 만들기
# stockcode, dartcode, name, market, gics, 연도, 전체이사수, 사외이사수, 사외이사비율

# 결과를 저장할 리스트
results = []

# 각 기업별로 데이터 처리
for _, corp in corps_df.iterrows():
    # 해당 기업의 파일 읽기
    corp_file = f"Data/corps/{corp['market']}_{corp['dartcode']}_{corp['name']}.csv"
    corp_data = pd.read_csv(corp_file)
    
    # metadata에서 해당 기업 정보 가져오기
    corp_meta = metadata[metadata['dartcode'] == corp['dartcode']].iloc[0]
    
    # 연도별로 그룹화하여 처리
    for year, year_data in corp_data.groupby('연도'):
        # 해당 연도의 마지막 데이터 사용 (가장 최신 분기 데이터)
        latest_data = year_data.iloc[-1]
        
        results.append({
            'stockcode': corp_meta['stockcode'],
            'dartcode': corp['dartcode'],
            'name': corp['name'],
            'market': corp['market'],
            'gics': corp_meta['gics'],
            '연도': year,
            '전체이사수': latest_data['전체이사수'],
            '사외이사수': latest_data['사외이사수'],
            '사외이사비율': latest_data['사외이사비율']
        })

# 결과를 데이터프레임으로 변환
final_df = pd.DataFrame(results)

# 결과 확인
print(final_df.head())
print(f"총 데이터 수: {len(final_df)}")

# stockcode 순으로 정렬
final_df = final_df.sort_values('stockcode')

# 연도별로 따로 저장
for year, year_data in final_df.groupby('연도'):
    year_data.to_csv(f'Data/directors_{year}.csv', index=False, encoding='utf-8-sig')
