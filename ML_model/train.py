import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, log_loss, classification_report
from sklearn.model_selection import GridSearchCV
import os
import warnings

# Suppress potential warnings from XGBoost
warnings.filterwarnings("ignore", category=UserWarning)

# --- 1. Data Preparation (The No-Leakage Zone) ---
print("--- Step 1: Loading and Preparing Data ---")

# Define the path to your data file
file_path = os.path.join('data', 'model_ready_data.csv')

try:
    df = pd.read_csv(file_path)
    print(f"✓ Successfully loaded '{file_path}'")
except FileNotFoundError:
    print(f"❌ Error: Could not find the file at '{file_path}'. Please ensure the script is in the correct directory.")
    exit()

# CRITICAL: Define your feature set using ONLY pre-game data to prevent leakage.
# These are the strongest predictors based on your correlation analysis.
feature_columns = [
    'rolling_avg_Points_diff',
    'rolling_avg_TotalNetYards_diff',
    'rolling_avg_Turnovers_diff',
    'days_of_rest_diff'
]
target_column = 'home_team_win'

# Create the final X (features) and y (target) datasets.
X = df[feature_columns]
y = df[target_column]
seasons = df['season']
print("✓ Features and target selected. Only pre-game data will be used.")

# --- 2. Chronological Data Split ---
print("\n--- Step 2: Splitting Data Chronologically ---")

# Use the most recent full season for testing to simulate real-world prediction.
test_season = df['season'].max()
train_seasons = df['season'].unique()[df['season'].unique() < test_season]

X_train = X[seasons < test_season]
X_test = X[seasons == test_season]
y_train = y[seasons < test_season]
y_test = y[seasons == test_season]

print(f"✓ Training on seasons: {list(train_seasons)}")
print(f"✓ Testing on season: {test_season}")
print(f"   Training data points: {len(X_train)}")
print(f"   Testing data points:  {len(X_test)}")

# --- 3. Hyperparameter Tuning with GridSearchCV ---
print("\n--- Step 3: Finding the Optimal Model ---")

# Define a 'grid' of hyperparameters to test for the XGBoost model.
# This grid covers the most impactful settings.
param_grid = {
    'n_estimators': [100, 200, 300],
    'max_depth': [3, 4, 5],
    'learning_rate': [0.01, 0.05, 0.1],
    'subsample': [0.7, 0.8, 1.0],
    'colsample_bytree': [0.7, 0.8, 1.0]
}

# Initialize GridSearchCV to test all combinations using 5-fold cross-validation.
grid_search = GridSearchCV(
    estimator=XGBClassifier(eval_metric='logloss', random_state=42),
    param_grid=param_grid,
    scoring='accuracy',  # We want to maximize prediction accuracy
    cv=5,                # 5-fold cross-validation
    verbose=1,
    n_jobs=-1            # Use all available CPU cores to speed up the process
)

print("⏳ Starting hyperparameter tuning... (This may take a few minutes)")
# Run the search on the TRAINING data only.
grid_search.fit(X_train, y_train)

# Get the best model found by the search.
best_model = grid_search.best_estimator_
print("✓ Tuning complete.")
print(f"\n🏆 Best Hyperparameters Found: {grid_search.best_params_}")

# --- 4. Final Evaluation ---
print("\n--- Step 4: Evaluating the Optimized Model ---")

# Use the best model from the grid search to make predictions on the unseen test data.
y_pred = best_model.predict(X_test)
y_pred_proba = best_model.predict_proba(X_test)[:, 1]

# Calculate final performance metrics.
accuracy = accuracy_score(y_test, y_pred)
logloss = log_loss(y_test, y_pred_proba)

print(f"\n--- Final Model Performance on {test_season} Season ---")
print(f"🎯 Accuracy: {accuracy:.4f} (Correctly predicted {accuracy:.2%} of games)")
print(f"📉 Log Loss: {logloss:.4f} (Lower is better, reflects probability accuracy)")
print("\n📋 Classification Report:")
print(classification_report(y_test, y_pred))