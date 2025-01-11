import OpenDartReader
import pandas as pd
import time

# DART API KEY
APIKEY = '62271078cde1657b39447a7444d1768d061bf9c5'
dart = OpenDartReader(APIKEY)

def save_corp_codes():
    """
    DART에 등록된 모든 기업의 고유번호 정보를 가져와서 CSV로 저장
    """
    try:
        print("기업 고유번호 정보 조회 중...")
        
        # corp_codes 속성을 통해 전체 기업 정보 가져오기
        corps_df = dart.corp_codes
        
        # metadata.csv 파일에서 상장기업 목록 읽기
        listed_corps = pd.read_csv('Data/metadata.csv', encoding='utf-8')
        
        # 종목코드를 문자열로 변환하고 6자리로 맞추기
        corps_df['stock_code'] = corps_df['stock_code'].fillna('').astype(str)
        corps_df['stock_code'] = corps_df['stock_code'].apply(lambda x: x.zfill(6) if x else '')
        
        listed_corps['종목코드'] = listed_corps['종목코드'].astype(str).str.zfill(6)
        
        # 상장기업만 필터링
        merged_df = pd.merge(
            corps_df,
            listed_corps,
            left_on=['stock_code', 'corp_name'],
            right_on=['종목코드', '종목명'],
            how='inner'
        )
        
        # 필요한 컬럼만 선택
        result_df = merged_df[['corp_code', 'corp_name', 'stock_code', '거래소명']].copy()
        result_df.columns = ['고유번호', '기업명', '종목코드', '거래소명']
        
        # CSV 파일로 저장
        output_path = 'Data/corp_codes.csv'
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"\n처리 완료:")
        print(f"- 전체 기업 수: {len(corps_df):,}개")
        print(f"- 상장기업 수: {len(result_df):,}개")
        print(f"- 저장 위치: {output_path}")
        
        return result_df
        
    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        return None

if __name__ == "__main__":
    save_corp_codes()
