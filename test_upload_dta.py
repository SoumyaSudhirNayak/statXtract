import requests

url = "http://127.0.0.1:8000/api/datasets/upload"
headers = {
    "Authorization": "Bearer YOUR_TOKEN_HERE" # Wait, I don't have a valid token right now. The API is authenticated.
}

with open("test.dta", "rb") as f:
    files = {"file": ("test.dta", f, "application/octet-stream")}
    data = {
        "dataset_name": "Test Stata Dataset",
        "description": "Test dataset for DTA upload",
        "tags": "test, stata"
    }
    # I will just post without token first to see if it responds 401
    response = requests.post(url, files=files, data=data)

print(response.status_code)
print(response.text)
