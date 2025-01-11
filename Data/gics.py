import pandas as pd

def load_and_merge_gics_data():
    # KOSPI, KOSDAQ GICS 데이터 로드 및 병합
    gics_kospi = pd.read_csv('./data_5854_20250111.csv', encoding='cp949', dtype={'종목코드': str})
    gics_kosdaq = pd.read_csv('./data_5901_20250111.csv', encoding='cp949', dtype={'종목코드': str})
    return pd.concat([gics_kospi, gics_kosdaq], ignore_index=True)

def process_stockcode(gics_df):
    # 종목코드 형식 통일화 (6자리 + 'A' 접두어)
    gics_df['stockcode'] = gics_df['종목코드'].apply(lambda x: f"A{x.zfill(6)}")
    return gics_df.sort_values(by='stockcode')

def update_metadata(gics_df):
    # 메타데이터 파일 읽기 전에 컬럼명 확인
    metadata = pd.read_csv('./metadata.csv', encoding='utf-8', dtype={'종목코드': str})
    print("현재 메타데이터 컬럼:", metadata.columns.tolist())  # 디버깅용
    
    metadata = metadata.sort_values(by='stockcode')
    metadata = pd.merge(metadata, gics_df, on='stockcode', how='left')
    
    # 업종명 -> gics 칼럼으로 변경
    metadata = metadata.rename(columns={'업종명': 'gics'})
    
    # 실제 존재하는 컬럼명으로 수정해주세요
    existing_columns = ['stockcode', 'avg_marcap', 'avg_amount', 'Name', 'Market', 'gics']  # 여기에 실제 존재하는 컬럼명을 추가
    metadata = metadata[existing_columns]
    
    return metadata

def main():
    # GICS 데이터 처리
    gics_df = load_and_merge_gics_data()
    gics_df = process_stockcode(gics_df)
    gics_df.to_csv('./gics.csv', index=False, encoding='utf-8')
    
    # 메타데이터 업데이트 및 저장
    metadata = update_metadata(gics_df)
    metadata.to_csv('./metadata.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    main()