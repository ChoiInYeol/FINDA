import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import cross_val_score
import shap
from sklearn.preprocessing import LabelEncoder
import matplotlib.font_manager as fm
import os
import platform

# 시각화 스타일 설정
plt.style.use('seaborn')
sns.set_palette("husl")
plt.rcParams['axes.unicode_minus'] = False

def load_data(year):
    # Load data
    df = pd.read_csv(f'./Data/FACTOR_G/final/factors_{year}.csv')
    
    # Ensure stockcode is 6-digit string
    df['stockcode'] = df['stockcode'].astype(str).str.zfill(6)
    
    # Rename columns to standardized names
    standard_cols = {
        f'외국인투자지분율_{year}': 'foreign_ownership',
        f'최대주주지분율_{year}': 'major_shareholder',
        f'OCF_{year}': 'ocf',
        f'debt_{year}': 'debt',
        f'asset_{year}': 'asset',
        f'intangible_{year}': 'intangible',
        'ESG등급_binary': 'esg_rating',
        '지배구조_binary': 'governance'
    }
    
    df = df.rename(columns=standard_cols)
    
    return df

def prepare_features(df, target_cols):
    # Separate features and target
    feature_cols = ['foreign_ownership', 'major_shareholder', 'ocf', 'debt', 'asset', 'intangible',
                   'sale_growth', 'ebitda_growth', 'div_growth', 'emp_growth']
    X = df[feature_cols]
    y = df[target_cols]
    
    # Fill NaN values
    X = X.fillna(X.mean())
    y = y.fillna(0)
    
    # Get company info
    info = df[['stockcode', 'name']]
    
    return X, y, feature_cols, info

def create_visualization_subplot(y_test, y_pred, model, X_test, feature_names, target_name, year):
    # Create a figure with subplots
    fig = plt.figure(figsize=(20, 12))
    
    # Confusion Matrix (top left)
    plt.subplot(2, 2, 1)
    cm = confusion_matrix(y_test, y_pred)
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.title(f'Confusion Matrix for {target_name} ({year})')
    
    # Feature Importance (top right)
    plt.subplot(2, 2, 2)
    importance_df = pd.DataFrame({
        'Feature': feature_names,
        'Importance': model.feature_importances_
    })
    importance_df = importance_df.sort_values('Importance', ascending=True)
    plt.barh(range(len(importance_df)), importance_df['Importance'])
    plt.yticks(range(len(importance_df)), importance_df['Feature'])
    plt.xlabel('Feature Importance')
    plt.title(f'Feature Importance for {target_name} ({year})')
    
    # SHAP Values (bottom)
    plt.subplot(2, 1, 2)
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)
    shap.summary_plot(shap_values, X_test, feature_names=feature_names, show=False)
    plt.title(f'SHAP Values for {target_name} ({year})')
    
    plt.tight_layout()
    plt.savefig(f'analysis_result_{target_name}_{year}.png', dpi=300, bbox_inches='tight')
    plt.close()

def train_and_evaluate(X_train, X_test, y_train, y_test, feature_names, target_name, year, train_info, test_info):
    # Create and train model
    model = XGBClassifier(
        learning_rate=0.1,
        n_estimators=100,
        max_depth=3,
        random_state=42
    )
    model.fit(X_train, y_train)
    
    # Make predictions
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    
    # Create integrated visualization
    create_visualization_subplot(y_test, y_pred, model, X_test, feature_names, target_name, year)
    
    # Get feature importance
    feature_importance = dict(zip(feature_names, model.feature_importances_))
    
    # Create results DataFrame
    results_df = pd.DataFrame({
        'Code': test_info['stockcode'],
        'Name': test_info['name'],
        'Actual': y_test,
        'Predicted': y_pred,
        'Probability': model.predict_proba(X_test)[:, 1]
    })
    
    return model, accuracy, feature_importance, results_df

def plot_yearly_performance(results_data):
    plt.figure(figsize=(15, 8))
    
    # Create DataFrame for plotting
    df = pd.DataFrame(results_data)
    
    # Plot ESG Rating accuracy
    plt.subplot(2, 1, 1)
    esg_data = df[df['Target'] == 'ESG Rating']
    plt.plot(esg_data['Year'], esg_data['Accuracy'], marker='o', label='Test Accuracy', linewidth=2)
    plt.plot(esg_data['Year'], esg_data['CV Score'], marker='s', label='CV Score', linewidth=2)
    plt.title('ESG Rating Prediction Performance by Year')
    plt.xlabel('Year')
    plt.ylabel('Score')
    plt.grid(True)
    plt.legend()
    
    # Plot Governance accuracy
    plt.subplot(2, 1, 2)
    gov_data = df[df['Target'] == 'Governance']
    plt.plot(gov_data['Year'], gov_data['Accuracy'], marker='o', label='Test Accuracy', linewidth=2)
    plt.plot(gov_data['Year'], gov_data['CV Score'], marker='s', label='CV Score', linewidth=2)
    plt.title('Governance Prediction Performance by Year')
    plt.xlabel('Year')
    plt.ylabel('Score')
    plt.grid(True)
    plt.legend()
    
    plt.tight_layout()
    plt.savefig('yearly_performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    results = []
    high_grade_predictions = pd.DataFrame()
    
    for year in range(2020, 2024):
        print(f"\n=== {year-1}년 데이터로 {year}년 예측 ===\n")
        
        # Load data
        train_data = load_data(year-1)
        test_data = load_data(year)
        
        # Define target columns
        target_cols = ['esg_rating', 'governance']
        
        # Prepare features
        X_train, y_train, feature_names, train_info = prepare_features(train_data, target_cols)
        X_test, y_test, _, test_info = prepare_features(test_data, target_cols)
        
        # Train and evaluate for each target
        for target_col in target_cols:
            target_name = 'ESG Rating' if target_col == 'esg_rating' else 'Governance'
            print(f"=== {target_name} Prediction for {year} ===")
            
            # Train and evaluate model
            model, accuracy, importance, pred_results = train_and_evaluate(
                X_train, X_test,
                y_train[target_col], y_test[target_col],
                feature_names, target_name, year,
                train_info, test_info
            )
            
            # Store results
            pred_results['Year'] = year
            pred_results['Target'] = target_name
            high_grade_predictions = pd.concat([high_grade_predictions, pred_results])
            
            # Print metrics
            print(f"Accuracy: {accuracy:.4f}\n")
            print("Classification Report:")
            print(classification_report(y_test[target_col], pred_results['Predicted']))
            
            # Cross validation
            cv_scores = cross_val_score(model, X_train, y_train[target_col], cv=5)
            print("\nCross Validation Scores:", cv_scores)
            cv_mean = np.mean(cv_scores)
            print("Average CV Score: {:.4f}\n".format(cv_mean))
            
            # Store performance metrics
            results.append({
                'Year': year,
                'Target': target_name,
                'Accuracy': accuracy,
                'CV Score': cv_mean
            })
    
    # Plot yearly performance comparison
    plot_yearly_performance(results)
    
    # Save high grade predictions
    high_grade_predictions[high_grade_predictions['Probability'] >= 0.7].to_csv('high_grade_companies.csv', index=False)
    print("\n상위 등급 기업 목록이 'high_grade_companies.csv'에 저장되었습니다.")

if __name__ == "__main__":
    main() 