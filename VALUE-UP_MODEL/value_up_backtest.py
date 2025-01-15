import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
import FinanceDataReader as fdr
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager, rc

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

# marcap 폴더를 Python 경로에 추가
WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(WORKSPACE_ROOT)

# marcap 모듈의 marcap_data 함수를 직접 구현
def marcap_data(start, end=None, code=None):
    start = pd.to_datetime(start)
    end = start if end==None else pd.to_datetime(end)
    df_list = []

    dtypes={'Code':np.str_, 'Name':np.str_, 
            'Open':np.int64, 'High':np.int64, 'Low':np.int64, 'Close':np.int64, 'Volume':np.int64, 'Amount':np.int64,
            'Changes':np.int64, 'ChangeCode':np.str_, 'ChagesRatio':np.float64, 'Marcap':np.int64, 'Stocks':np.int64,
            'MarketId':np.str_, 'Market':np.str_, 'Dept':np.str_,
            'Rank':np.int64}
      
    for year in range(start.year, end.year + 1):
        try:
            csv_file = os.path.join(WORKSPACE_ROOT, 'marcap', 'data', f'marcap-{year}.csv.gz')
            df = pd.read_csv(csv_file, dtype=dtypes, parse_dates=['Date'])
            df_list.append(df)
        except Exception as e:
            print(e)
            pass
    
    if not df_list:
        return pd.DataFrame()
        
    df_merged = pd.concat(df_list)
    df_merged = df_merged[(start <= df_merged['Date']) & (df_merged['Date'] <= end)]  
    df_merged = df_merged.sort_values(['Date','Rank'])
    if code:
        df_merged = df_merged[code == df_merged['Code']]  
    return df_merged[df_merged['Volume'] > 0]

def load_constituents(year):
    """연도별 구성종목 데이터를 로드하는 함수"""
    file_path = f'VALUE-UP_MODEL/value_up_constituents_{year}.csv'
    if os.path.exists(file_path):
        print(f"{year}년 구성종목 파일 로드: {file_path}")
        df = pd.read_csv(file_path)
        # stockcode에서 'A' 제거
        df['stockcode'] = df['stockcode'].str[1:]
        return df
    print(f"{year}년 구성종목 파일이 없습니다: {file_path}")
    return None

def calculate_cap_factor(weights):
    """
    시가총액 비중상한(CAP) 15% 적용
    """
    # 0이나 NaN이 있는 경우 제거
    valid_weights = weights[weights > 0].dropna()
    if len(valid_weights) == 0:
        return pd.Series(0.0, index=weights.index)
        
    cap_factors = pd.Series(1.0, index=valid_weights.index)
    while True:
        adj_weights = valid_weights * cap_factors
        adj_weights = adj_weights / adj_weights.sum()
        if adj_weights.max() <= 0.15:  # 15% CAP
            break
        max_idx = adj_weights.idxmax()
        cap_factors[max_idx] *= 0.15 / adj_weights[max_idx]
    
    # 원래 인덱스로 복원
    full_cap_factors = pd.Series(0.0, index=weights.index)
    full_cap_factors[cap_factors.index] = cap_factors
    return full_cap_factors

def calculate_portfolio_return(start_date='2021-06-01', end_date='2024-12-31'):
    """
    포트폴리오 수익률 계산 함수
    - 각 연도의 구성종목은 전년도 데이터를 기반으로 선정됨
    - 2021년 6월: 2020년 데이터로 선정된 종목으로 투자
    - 2022년 6월: 2021년 데이터로 선정된 종목으로 리밸런싱
    - 2023년 6월: 2022년 데이터로 선정된 종목으로 리밸런싱
    - 2024년 6월: 2023년 데이터로 선정된 종목으로 리밸런싱
    """
    print("포트폴리오 수익률 계산 시작...")
    
    # 날짜를 datetime 객체로 변환
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    print(f"주가 데이터 로드 중... ({start_date} ~ {end_date})")
    # 전체 기간의 주가 데이터 로드
    market_data = marcap_data(start_date, end_date)
    print(f"로드된 주가 데이터: {len(market_data)}행")
    
    # 연도별 구성종목 데이터 로드
    constituents_by_year = {}
    for year in range(2020, 2024):  # 2020년부터 2023년까지
        constituents = load_constituents(year)  # 전년도 데이터로 선정된 종목
        if constituents is not None:
            constituents_by_year[year] = constituents
            print(f"{year}년 구성종목: {len(constituents)}개")
    
    # 일별 수익률 계산을 위한 빈 데이터프레임
    dates = pd.date_range(start=start_date, end=end_date, freq='B')
    portfolio_return = pd.Series(index=dates, dtype=float)
    
    # 일별 포트폴리오 수익률 계산
    print("\n일별 수익률 계산 중...")
    valid_dates = 0
    prev_date = None
    current_constituents = None
    
    for date in dates:
        # 6월 1일에 리밸런싱
        if date.month == 6 and date.day == 1:
            year = date.year
            if year in constituents_by_year:
                current_constituents = constituents_by_year[year]
                print(f"\n{year}년 6월 리밸런싱 - 구성종목 수: {len(current_constituents)}개")
        
        if current_constituents is None:
            continue
            
        current_market_data = market_data[market_data['Date'] == date]
        if current_market_data.empty:
            continue
            
        # 현재 시점의 시가총액 계산
        current_market_cap = pd.Series(0.0, index=current_constituents['stockcode'])
        for code in current_constituents['stockcode']:
            stock_data = current_market_data[current_market_data['Code'] == code]
            if not stock_data.empty:
                current_market_cap[code] = stock_data['Marcap'].iloc[0]
        
        # 시가총액이 0인 종목 제거
        current_market_cap = current_market_cap[current_market_cap > 0]
        if len(current_market_cap) == 0:
            continue
            
        # 시가총액 가중치 계산 (CAP 15% 적용)
        weights = current_market_cap / current_market_cap.sum()
        cap_factors = calculate_cap_factor(weights)
        weights = weights * cap_factors
        weights = weights / weights.sum()
        
        # 수익률 계산
        if prev_date is not None:
            prev_market_data = market_data[market_data['Date'] == prev_date]
            daily_return = 0.0
            
            for code in weights.index:
                curr_data = current_market_data[current_market_data['Code'] == code]
                prev_data = prev_market_data[prev_market_data['Code'] == code]
                
                if not curr_data.empty and not prev_data.empty:
                    stock_return = curr_data['Close'].iloc[0] / prev_data['Close'].iloc[0] - 1
                    daily_return += stock_return * weights[code]
            
            portfolio_return[date] = daily_return
            valid_dates += 1
        
        prev_date = date
    
    print(f"계산 완료된 거래일 수: {valid_dates}일")
    
    # 누적 수익률 계산
    portfolio_return = portfolio_return.dropna()
    cumulative_return = (1 + portfolio_return).cumprod()
    return cumulative_return

def normalize_to_base_date(return_series, base_date, base_value=1000):
    """
    특정 기준일의 값을 기준으로 전체 시계열을 정규화
    - 기준일 이전: 기준일 값에서 역산
    - 기준일 이후: 기준일 값에서 순산
    """
    # 기준일의 누적 수익률 값
    base_return = return_series[base_date]
    
    # 전체 시계열을 기준일 기준으로 정규화
    normalized_series = return_series / base_return * base_value
    return normalized_series

def load_benchmark_data(start_date, end_date, base_date):
    """벤치마크 지수 데이터 로드"""
    benchmarks = {
        'KOSPI': 'KS11',
        'KOSDAQ': 'KQ11',
        'KOSPI200': 'KS200'
    }
    
    benchmark_data = {}
    for name, code in benchmarks.items():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            # 기준일 기준으로 정규화
            df['Return'] = df['Close'] / df['Close'].shift(1)
            df['CumReturn'] = df['Return'].fillna(1).cumprod()
            normalized_series = normalize_to_base_date(df['CumReturn'], base_date)
            benchmark_data[name] = normalized_series
        except Exception as e:
            print(f"Error loading {name}: {e}")
    
    return benchmark_data

def plot_comparison(value_up_index, benchmark_data):
    """지수 비교 그래프 생성"""
    plt.figure(figsize=(15, 8))
    
    # 밸류업 지수 플롯
    plt.plot(value_up_index.index, value_up_index.values, label='밸류업 지수', linewidth=2)
    
    # 벤치마크 지수 플롯
    colors = ['#FF9999', '#66B2FF', '#99FF99']
    for (name, data), color in zip(benchmark_data.items(), colors):
        # 날짜 인덱스 맞추기
        data = data.reindex(value_up_index.index, method='ffill')
        plt.plot(data.index, data.values, label=name, linewidth=2, alpha=0.7, color=color)
    
    plt.title('밸류업 지수 vs 벤치마크 지수 비교 (2024-01-02=1000)', fontsize=14, pad=20)
    plt.xlabel('날짜', fontsize=12)
    plt.ylabel('지수 (2024-01-02=1000)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)
    
    # 그래프 저장
    plt.savefig('value_up_return_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

def calculate_performance_metrics(value_up_index, benchmark_data):
    """성과 지표 계산"""
    results = pd.DataFrame(columns=['전체 수익률', '연평균 수익률', '변동성'])
    
    # 밸류업 지수 성과 계산
    value_up_returns = value_up_index.pct_change().dropna()
    total_return = (value_up_index.iloc[-1] / value_up_index.iloc[0] - 1) * 100
    annual_return = (1 + total_return/100) ** (252/len(value_up_index)) - 1
    volatility = value_up_returns.std() * np.sqrt(252) * 100
    
    results.loc['밸류업 지수'] = [
        f"{total_return:.2f}%",
        f"{annual_return*100:.2f}%",
        f"{volatility:.2f}%"
    ]
    
    # 벤치마크 지수 성과 계산
    for name, data in benchmark_data.items():
        data = data.reindex(value_up_index.index, method='ffill')
        returns = data.pct_change().dropna()
        total_return = (data.iloc[-1] / data.iloc[0] - 1) * 100
        annual_return = (1 + total_return/100) ** (252/len(data)) - 1
        volatility = returns.std() * np.sqrt(252) * 100
        
        results.loc[name] = [
            f"{total_return:.2f}%",
            f"{annual_return*100:.2f}%",
            f"{volatility:.2f}%"
        ]
    
    return results

def main():
    # 기준일 설정
    base_date = pd.to_datetime('2024-01-02')
    
    # 포트폴리오 수익률 계산
    portfolio_return = calculate_portfolio_return()
    
    # 기준일 기준으로 정규화
    normalized_portfolio = normalize_to_base_date(portfolio_return, base_date)
    
    # 벤치마크 데이터 로드
    start_date = portfolio_return.index.min()
    end_date = portfolio_return.index.max()
    benchmark_data = load_benchmark_data(start_date, end_date, base_date)
    
    # 결과 저장
    portfolio_df = pd.DataFrame({
        'Date': normalized_portfolio.index,
        'Index': normalized_portfolio.values
    })
    portfolio_df.to_csv('value_up_return_result.csv', index=False)
    
    # 기본 통계 출력
    print("\n=== 밸류업 포트폴리오 백테스트 결과 ===")
    print(f"시작일: {portfolio_df['Date'].min()}")
    print(f"종료일: {portfolio_df['Date'].max()}")
    print(f"기준일(2024-01-02) 지수: 1000.00")
    
    # 연도별 수익률 계산
    yearly_returns = []
    for year in range(2021, 2025):  # 2021년부터 2024년까지
        year_data = portfolio_df[portfolio_df['Date'].dt.year == year]
        if not year_data.empty:
            yearly_return = ((year_data['Index'].iloc[-1] / year_data['Index'].iloc[0]) - 1) * 100
            yearly_returns.append((year, yearly_return))
    
    print("\n=== 연도별 수익률 ===")
    for year, ret in yearly_returns:
        print(f"{year}년: {ret:.2f}%")
    
    # 성과 비교
    print("\n=== 성과 비교 (2024-01-02 기준) ===")
    results = pd.DataFrame(columns=['시작 대비 기준일', '연평균 수익률', '변동성'])
    
    # 밸류업 지수 성과 계산
    start_to_base = ((1000 / normalized_portfolio.iloc[0]) - 1) * 100
    days = (base_date - normalized_portfolio.index[0]).days
    annual_return = ((1 + start_to_base/100) ** (365/days) - 1) * 100
    returns = normalized_portfolio.pct_change().dropna()
    volatility = returns.std() * np.sqrt(252) * 100
    
    results.loc['밸류업 지수'] = [
        f"{start_to_base:.2f}%",
        f"{annual_return:.2f}%",
        f"{volatility:.2f}%"
    ]
    
    # 벤치마크 지수 성과 계산
    for name, data in benchmark_data.items():
        data = data.reindex(normalized_portfolio.index, method='ffill')
        start_to_base = ((1000 / data.iloc[0]) - 1) * 100
        annual_return = ((1 + start_to_base/100) ** (365/days) - 1) * 100
        returns = data.pct_change().dropna()
        volatility = returns.std() * np.sqrt(252) * 100
        
        results.loc[name] = [
            f"{start_to_base:.2f}%",
            f"{annual_return:.2f}%",
            f"{volatility:.2f}%"
        ]
    
    print(results)
    
    # 그래프 생성
    plot_comparison(normalized_portfolio, benchmark_data)
    print("\n그래프가 'value_up_return_comparison.png' 파일로 저장되었습니다.")

if __name__ == "__main__":
    main()
