# 메타데이터 추출
import pandas as pd
import os

def create_metadata_from_files():
    # 데이터 저장 경로
    base_path = 'Data'
    markets = ['KOSPI', 'KOSDAQ']
    
    # 메타데이터를 저장할 리스트
    metadata_list = []
    
    # 각 시장별로 파일 탐색
    for market in markets:
        market_path = os.path.join(base_path, market)
        
        # 해당 경로가 존재하는 경우에만 처리
        if os.path.exists(market_path):
            # 디렉토리 내의 모든 CSV 파일 탐색
            for filename in os.listdir(market_path):
                if filename.endswith('.csv'):
                    # 파일명에서 종목코드와 종목명 추출
                    # 형식: "종목코드_종목명.csv"
                    code_name = filename.replace('.csv', '')
                    code, name = code_name.split('_', 1)
                    
                    # 메타데이터 리스트에 추가
                    metadata_list.append({
                        '거래소명': market,
                        '종목코드': code,
                        '종목명': name
                    })
    
    # 데이터프레임 생성
    metadata_df = pd.DataFrame(metadata_list)
    
    # 종목코드 기준으로 정렬
    metadata_df = metadata_df.sort_values(['거래소명', '종목코드'])
    
    # 인덱스 리셋
    metadata_df = metadata_df.reset_index(drop=True)
    
    return metadata_df

# 메타데이터 생성
metadata = create_metadata_from_files()

# 메타데이터 정보 출력
print("=== 메타데이터 정보 ===")
print(f"총 종목 수: {len(metadata)}")
print("\n거래소별 종목 수:")
print(metadata['거래소명'].value_counts())
print("\n=== 메타데이터 미리보기 ===")
print(metadata.head())

# 메타데이터 저장
metadata.to_csv('Data/metadata.csv', index=False, encoding='utf-8-sig')