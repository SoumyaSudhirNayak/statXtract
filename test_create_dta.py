import pandas as pd
import requests

df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
df.to_stata("test.dta")

print("Created test.dta")
