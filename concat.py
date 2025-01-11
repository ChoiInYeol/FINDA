import pandas as pd

# 파일 읽기 (모든 컬럼을 문자열로 읽기)
corp_codes = pd.read_csv('Data/temp/corp_codes.csv', dtype=str)
metadata = pd.read_csv('Data/temp/metadata.csv', dtype=str)

# 종목코드 순으로 정렬
# corp_codes는 '종목코드'
# metadata는 'stockcode'

corp_codes = corp_codes.sort_values('종목코드')
metadata = metadata.sort_values('stockcode')

# matedata 행에 market 컬럼 추가
# corp_codes에서 '거래소명' 컬럼을 이용해 종목코드 == stockcode인 행을 찾아서 market 컬럼 추가

# metadata에 market 컬럼 추가
metadata['market'] = metadata['stockcode'].map(
    corp_codes.set_index('종목코드')['거래소명']
)

# 컬럼 순서 정리
metadata = metadata[['stockcode', 'dartcode', 'name', 'market', 'gics']]

print(metadata.head())

# 결측치는 NaN으로 채우기
metadata = metadata.fillna('NaN')

metadata.to_csv('Data/metadata2.csv', index=False, encoding='utf-8-sig')