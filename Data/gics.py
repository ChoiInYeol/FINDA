import pandas as pd

gics_df = pd.read_csv('total_gics.csv', encoding='utf-8')
metadata = pd.read_csv('metadata.csv', encoding='utf-8')

# gics_df의 gics 컬럼으로 metadata의 gics 컬럼을 업데이트
metadata = pd.merge(metadata, gics_df[['stockcode', 'gics']], on='stockcode', how='left', suffixes=('_old', ''))
metadata['gics'] = metadata['gics'].fillna(metadata['gics_old'])
metadata = metadata.drop('gics_old', axis=1)

# metadata를 다시 저장
metadata.to_csv('metadata.csv', index=False, encoding='utf-8')
