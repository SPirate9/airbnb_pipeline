import pandas as pd, os

df = pd.read_csv('data/reviews.csv')
os.makedirs('data/reviews_chunks', exist_ok=True)

for i in range(0, len(df), 1000):
    df.iloc[i:i+1000].to_csv(f'data/reviews_chunks/reviews_{i//1000}.csv', index=False)
