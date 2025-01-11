import OpenDartReader
import pandas as pd
import time
import os

# DART API KEY
APIKEY = '62271078cde1657b39447a7444d1768d061bf9c5'
dart = OpenDartReader(APIKEY)

def get_outside_director_ratio():
    """전체 기업의 연간 사외이사 비율을 조회하는 함수"""
    try:
        # CSV에서 전체 기업 데이터 읽기 및 KOSPI/KOSDAQ 정렬
        corps_df = pd.read_csv('Data/corp_codes.csv', encoding='utf-8-sig')
        corps_df['고유번호'] = corps_df['고유번호'].astype(str).str.zfill(8)
        
        # KOSPI/KOSDAQ 기업만 필터링하고 KOSPI 우선 정렬
        corps_df = corps_df[corps_df['거래소명'].isin(['KOSPI', 'KOSDAQ'])].copy()
        corps_df['시장순서'] = corps_df['거래소명'].map({'KOSPI': 0, 'KOSDAQ': 1})
        corps_df = corps_df.sort_values(['시장순서', '고유번호']).drop('시장순서', axis=1)
        
        total_companies = len(corps_df)
        processed_count = 0
        skipped_count = 0
        
        # 이미 처리된 기업 확인을 위한 디렉토리 생성
        os.makedirs('Data/corps', exist_ok=True)
        existing_files = set(os.listdir('Data/corps'))
        
        for idx, (corp_code, corp_name, stock_code, market) in enumerate(corps_df.values, 1):
            # 이미 처리된 파일 확인
            expected_filename = f"{market}_{corp_code}_{corp_name}.csv"
            if expected_filename in existing_files:
                skipped_count += 1
                print(f"SKIP - [{market}] {corp_name}({corp_code})")
                continue
                
            print(f"\n=== ({idx}/{total_companies}) [{market}] {corp_name}({corp_code}) 검색 중... ===")
            results = []
            
            try:
                # 2020년부터 2023년까지의 사업보고서 목록 조회
                reports = dart.list(corp_code, start='2020-01-01', end='2024-03-31', kind='A')
                
                if reports is not None and not reports.empty:
                    report_dict = {}
                    
                    for _, report in reports.iterrows():
                        report_nm = report['report_nm']
                        
                        # 사업보고서만 처리
                        if '사업보고서' in report_nm and '반기' not in report_nm and '분기' not in report_nm:
                            # 보고서명에서 연도 추출
                            if '(' in report_nm and ')' in report_nm:
                                date_part = report_nm.split('(')[1].split(')')[0]
                                try:
                                    year = date_part.split('.')[0].strip()
                                    
                                    # 2020~2023년 데이터만 처리
                                    if 2020 <= int(year) <= 2023:
                                        # 기재정정 보고서가 있으면 우선 처리
                                        key = f"{year}_사업보고서"
                                        if key not in report_dict or '[기재정정]' in report_nm:
                                            report_dict[key] = {
                                                'year': year,
                                                'report_type': '사업보고서',
                                                'report_code': '11011'
                                            }
                                except:
                                    continue
                    
                    # 정리된 보고서 정보로 데이터 조회
                    for report_info in report_dict.values():
                        year = report_info['year']
                        report_type = report_info['report_type']
                        report_code = report_info['report_code']
                        
                        try:
                            data = dart.report(corp_code, '사외이사', year, report_code)
                            
                            if data is not None and not data.empty:
                                for _, row in data.iterrows():
                                    try:
                                        total_directors = int(row['drctr_co']) if row['drctr_co'] != '-' else 0
                                        outside_directors = int(row['otcmp_drctr_co']) if row['otcmp_drctr_co'] != '-' else 0
                                        ratio = outside_directors / total_directors if total_directors > 0 else 0
                                        
                                        results.append({
                                            '시장구분': market,
                                            '연도': year,
                                            '분기': '사업',
                                            '기준일자': row['stlm_dt'],
                                            '전체이사수': total_directors,
                                            '사외이사수': outside_directors,
                                            '사외이사비율': round(ratio, 4)
                                        })
                                        
                                        print(f"- [{market}] {year} {report_type}({row['stlm_dt']}): 전체이사 {total_directors}명, 사외이사 {outside_directors}명 ({ratio:.2%})")
                                    except (ValueError, TypeError) as e:
                                        print(f"- {year} {report_type} 데이터 처리 실패: {str(e)}")
                            
                            time.sleep(0.1)
                            
                        except Exception as e:
                            print(f"- {year} {report_type} 조회 실패: {str(e)}")
                
                # 기업별 결과 저장
                if results:
                    df = pd.DataFrame(results)
                    filename = f'Data/corps/{market}_{corp_code}_{corp_name}.csv'
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"\n[{market}] {corp_name} 결과 저장 완료: {len(results)}건")
                    processed_count += 1
                else:
                    print(f"\n[{market}] {corp_name} 조회된 데이터 없음")
                    
            except Exception as e:
                print(f"- 정기공시 목록 조회 실패: {str(e)}")
            
            time.sleep(0.2)
            
        print(f"\n처리 완료: 전체 {total_companies}개 중 {processed_count}개 처리, {skipped_count}개 건너뜀")
            
    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
        return None

if __name__ == "__main__":
    get_outside_director_ratio()