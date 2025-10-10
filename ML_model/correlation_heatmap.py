import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Define the path to your data.
# If your script is in the same folder as the 'data' folder, this should work.
file_path = os.path.join('data', 'model_ready_data.csv')

try:
    # Load the dataset
    df = pd.read_csv(file_path)
    print(f"Successfully loaded '{file_path}'")

    # Calculate the correlation of all numeric columns with the target 'home_team_win'
    correlations = df.corr(numeric_only=True)['home_team_win'].dropna()

    # Sort by absolute value to find the strongest relationships
    sorted_correlations = correlations.abs().sort_values(ascending=False)

    # Get the sorted features (excluding the target itself)
    top_features = correlations.loc[sorted_correlations.index].drop('home_team_win')

    # Separate top 15 positive and top 15 negative correlations for a balanced plot
    top_positive = top_features[top_features > 0].head(15)
    top_negative = top_features[top_features < 0].tail(15)

    # Combine for plotting
    top_corr_for_plot = pd.concat([top_positive, top_negative.sort_values(ascending=True)])

    # --- Plotting ---
    plt.figure(figsize=(12, 14))
    sns.barplot(x=top_corr_for_plot.values, y=top_corr_for_plot.index, palette="vlag")
    plt.title('Top 30 Features Correlated with Home Team Win', fontsize=16)
    plt.xlabel('Correlation Coefficient', fontsize=12)
    plt.ylabel('Features', fontsize=12)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('full_correlation_barplot.png')
    print("Successfully generated and saved 'full_correlation_barplot.png'")

    # --- Printing Top Features ---
    print("\n--- Correlation Analysis Results ---")
    print("\nTop 10 Most Predictive Features (Positive Correlation):")
    print(top_positive.head(10))
    print("\nTop 10 Most Predictive Features (Negative Correlation):")
    print(top_negative.sort_values(ascending=True).head(10))

except FileNotFoundError:
    print(f"Error: Could not find the file at '{file_path}'.")
    print("Please make sure the script is run from the correct directory.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")