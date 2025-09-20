import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Load your model-ready data
try:
    DATA_DIR = 'data'
    MODEL_DATA_FILE = os.path.join(DATA_DIR, "model_ready_data.csv")
    df = pd.read_csv(MODEL_DATA_FILE)
    print("Successfully loaded model_ready_data.csv")
except FileNotFoundError:
    print("Error: 'model_ready_data.csv' not found. Make sure the file is in the correct directory.")
    exit()

# Select a subset of features for a readable heatmap
# We'll focus on the engineered rolling averages and the target variable
features_to_analyze = [
    'home_team_win', # This is our target
    'home_rolling_avg_Points',
    'away_rolling_avg_Points',
    'rolling_avg_Points_diff',
    'home_rolling_avg_TotalNetYards',
    'away_rolling_avg_TotalNetYards',
    'rolling_avg_TotalNetYards_diff',
    'home_rolling_avg_Turnovers',
    'away_rolling_avg_Turnovers',
    'rolling_avg_Turnovers_diff'
]

# Calculate the correlation matrix
corr_matrix = df[features_to_analyze].corr()

# Set up the matplotlib figure
plt.figure(figsize=(12, 10))

# Draw the heatmap
sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap='coolwarm', linewidths=.5)

# Add a title and save the figure
plt.title('Correlation Matrix of Rolling Average Features', fontsize=16)
plt.savefig('correlation_heatmap.png')

print("Correlation heatmap has been saved as 'correlation_heatmap.png'")