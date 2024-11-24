import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import make_pipeline

# Load the dataset
file_path = 'CPLG_Settlement_Numbers.csv'
data = pd.read_csv(file_path)

# Drop rows with missing values
data_cleaned = data.dropna()

# Define features and target variable
X_cleaned = data_cleaned[['Make', 'Model', 'Year', 'Transmission', 'Engine', 'Electrical', 'Brakes', 'Structural', 'HVAC']]
y_cleaned = data_cleaned['Amount']

# One-hot encoding for categorical features
categorical_features = ['Make', 'Model']
preprocessor = ColumnTransformer(transformers=[('cat', OneHotEncoder(), categorical_features)], remainder='passthrough')

# Splitting the dataset into training and testing sets
X_train_cleaned, X_test_cleaned, y_train_cleaned, y_test_cleaned = train_test_split(X_cleaned, y_cleaned, test_size=0.2, random_state=42)

# Creating the pipeline
model_cleaned = make_pipeline(preprocessor, LinearRegression())

# Fit the model
model_cleaned.fit(X_train_cleaned, y_train_cleaned)

# Get the coefficients and feature names
coefficients_cleaned = model_cleaned.named_steps['linearregression'].coef_
feature_names_cleaned = model_cleaned.named_steps['columntransformer'].get_feature_names_out()

# Get the intercept
intercept = model_cleaned.named_steps['linearregression'].intercept_

# Create a DataFrame for the coefficients
coefficients_cleaned_df = pd.DataFrame({'Feature': feature_names_cleaned, 'Coefficient': coefficients_cleaned})

# Add the intercept to the DataFrame
intercept_df = pd.DataFrame({'Feature': ['Intercept'], 'Coefficient': [intercept]})

# Combine intercept and coefficients into a single DataFrame
final_coefficients_df = pd.concat([intercept_df, coefficients_cleaned_df], ignore_index=True)

# Output the result to a CSV file
final_coefficients_df.to_csv('calc_Coefficients_with_Intercept.csv', index=False)
print("Coefficient calculations including intercept have been saved to calc_Coefficients_with_Intercept.csv")
