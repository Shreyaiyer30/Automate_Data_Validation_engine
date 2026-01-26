import pandas as pd
import numpy as np
from pathlib import Path

# Create sample data directory
Path('data').mkdir(exist_ok=True)

# Generate sample customer data with quality issues
np.random.seed(42)
n_records = 1000

sample_data = {
    'customer_id': range(1, n_records + 1),
    'name': np.random.choice(['John', 'Jane', 'Bob', 'Alice', 'charlie', 'DAVE'], n_records),
    'age': np.random.choice(list(range(18, 80)) + [None, None], n_records),
    'email': [f"customer{i}@email.com" if i % 10 != 0 else None for i in range(n_records)],
    'salary': np.random.normal(60000, 20000, n_records),
    'join_date': pd.date_range('2020-01-01', periods=n_records, freq='D'),
    'status': np.random.choice(['active', 'inactive', 'pending', None], n_records),
    'purchase_count': np.random.poisson(5, n_records)
}

df_sample = pd.DataFrame(sample_data)

# Add some quality issues
# - Missing values
df_sample.loc[np.random.choice(df_sample.index, 50, replace=False), 'age'] = None
df_sample.loc[np.random.choice(df_sample.index, 30, replace=False), 'salary'] = None

# - Outliers in salary
outlier_indices = np.random.choice(df_sample.index, 20, replace=False)
df_sample.loc[outlier_indices, 'salary'] = df_sample.loc[outlier_indices, 'salary'] * 10

# - Duplicates
duplicates = df_sample.head(10).copy()
df_sample = pd.concat([df_sample, duplicates], ignore_index=True)

# - Invalid ages
df_sample.loc[np.random.choice(df_sample.index, 15, replace=False), 'age'] = 200

# Save sample data
df_sample.to_csv('data/sample_data.csv', index=False)
print(f"Sample data created: data/sample_data.csv ({len(df_sample)} rows)")

# Create a clean version for comparison
df_clean = df_sample.drop_duplicates().copy()
df_clean = df_clean.dropna(subset=['age', 'email', 'salary'])
df_clean.loc[df_clean['age'] > 120, 'age'] = df_clean.loc[df_clean['age'] > 120, 'age'].median()
df_clean = df_clean.reset_index(drop=True)

df_clean.to_csv('data/sample_data_clean.csv', index=False)
print(f"Clean sample data created: data/sample_data_clean.csv ({len(df_clean)} rows)")
