import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("data/listings.csv")
engine = create_engine("postgresql://saad:saad@localhost:5432/airbnb")
df.to_sql("listings", engine, if_exists="replace", index=False)