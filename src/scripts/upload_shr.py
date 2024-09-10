import json
import requests
from msal import ConfidentialClientApplication

client_id = "14940b01-4019-4c38-9bc2-f945791706e1"
secret_value= "6aL8Q~IOgjERm.zeAmJ0ora8NkNUlWhU8_oSgcpa"
cert_thumbprint = "948F259EA3439EE23EEB5328BD72B6FBE17C6699"
tenant_id = "4f2d8600-9eba-41f7-806e-d36ce4acb7a0"

authority = f"https://login.microsoftonline.com/{tenant_id}"

private_key = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC7MZBmIJ0ByKCZ
nb0sU4w39bbeXVMhdPn3UBId9fktqhRk1eJgANzOiqAx/Y+FDB+/mqLWYEyq2cGG
+RPKIy/Ig2rCLqEBC6VlPdbWpAPLjlKu990GH+zpkfpMp+dCP9qRmAPFlUSzJrWP
+TrasZGdTi4tpNkSAbv1BwqnklsT/efwZRlSwxK9ptsQFVzW5fHT8QtC0Xh0PaK5
54HdJQ6n516fUdunJQLCM7c32xrtCxh7PhClcQh6kEND6DGS740G8y1FS2EEVmTj
l0Rte+9Xr/IQclrZ6YcWrlbHUJMKGnRtBqsYOeHFRX5D6KYtCuLTzwmtnn+Q3Dj7
UhPLnqlHAgMBAAECggEAWdxasbQBOvx5IBUpXVCEFM73KLrLL9nsLw04jTppkkK9
xIQqRiTT2rCQYz8R+PAPg5azic6zryaWcPk7x0Lp6ssvYiUpNAvHq5iqe+JDUiGx
zn19FDYaMCvb6JWHffWSMmczlFNqJcg0y3b8ikRInyeLIm0qqSq89EybkELBRxA/
bOt4htUsSf3zlcXcXiiiLZ5MgQuWRC2bqnY5ObM7Wfrc2QDnxr8x7OkCz/oqqX+r
iu4kzlkIO3mtlCaLH3JIr0M17MkIN75iKia8ApkSmF793ZYBjRF127LlfpSKrS8K
o8KBGQtz02P/9d/t5X7hCjRBdSbKOvbIIJrvfouh/QKBgQDaerYSf2PYvrFYCuHH
zglTVIRtKiCqJR/LmehBs8BJQ3YMQHFpHzy55vDkUShx/+6tzwzaRuJpakFReJH1
bBmMRDV/f6QDbt0JW2YGGljyhN/t9wSVRshhG3nJ64wRwzfV4KOBb7GTj1Y3u1W/
P49wk4UokPpSq4RQsm2joAaSUwKBgQDbV2XepVWO/uNN4OsP+aQj5py2H21wjNq7
9Vrj3s3wkhQOAdJme4frJdSrPZhX01mbpkh6jx8E55Udd8yrZTElhBj/MmdQTDS7
usQLjuZKbaVxw3oKompEAqDd6LvhrSOxoK0fbv1qvrjjRNpdXXOlgbBXbFv8vfN/
bxVubdiWvQKBgDlkgu/ZByGo9m1qbmVOeqSpTyBeMLaBihiyFZEs5xZX4mrVgvVa
f1lWkXFo4HcSBGEkQvwUIuYOQ5pjUfRmsU9nm6YiobNFLEuI9wQjZ7pNrYWVnl6Y
eYsI7LVeay0/WyuUF1+pN7zLqpp5W80hpUytdA10CE4vQFLyjFqvbwqBAoGBAL9C
7558tyWQ1y5isTZl7h4sCny43NpmyJlbclz/PL7I8lngdtJMZ5HBlDeZ50y2DA4w
qkTMpjTFp35hp4PRIlDfZlipX/Nh8B2+1xJpEiDWiYyw8qtxQo55aEL4nVRFgLl0
LWIaiznYgyoSMQN26M/qTZV99JxoGnsFpz9644FVAoGBANCiqohpzIBkGjUNgl1L
fA9CgzlwTiAbhHIjl7qV4FEebGAmDnbZNyQX6eidK8Ybd3t7dl+BG4GQKp5VtZQ6
3MBAysQGXhexi+HGGpVCCB7Uj1fBKL9/rADXyXPJDtfkfZU10IakC4TF43IJfeA1
wrv8+6u6Jg4dc1wYHJeYyQgq
-----END PRIVATE KEY-----"""

cert = {
    "private_key": private_key,
    "thumbprint": cert_thumbprint,
}

msal_app = ConfidentialClientApplication(
    client_id=client_id,
    authority=authority,
    client_credential=cert,
)

scopes_sharepoint_online = ["https://ceoxplan.sharepoint.com/.default"]

results = msal_app.acquire_token_for_client(scopes_sharepoint_online)

if "access_token" in results:
    access_token = results.get("access_token")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json;odata=verbose",
        "Content-Type": "application/json",
    }

    sharepoint_base_url = "https://ceoxplan.sharepoint.com/sites/AtividadesTI"
    folder_url = "Documentos Compartilhados/XML Plataforma"    
    encoded_folder_url = requests.utils.quote(folder_url, safe='') 
    api_url = f"{sharepoint_base_url}/_api/web/GetFolderByServerRelativeUrl('{encoded_folder_url}')/Files"
    
    response = requests.get(url=api_url, headers=headers)
    
    if response and response.status_code == 200:           
        files = response.json().get("d", {}).get("results", [])
        
        for file in files:
            file_name = file["Name"]
            file_url = file["ServerRelativeUrl"]
            file_download_url = f"{sharepoint_base_url}/_api/web/GetFileByServerRelativeUrl('{requests.utils.quote(file_url, safe='')}')/$value"
            
            # Download the file
            file_response = requests.get(url=file_download_url, headers=headers, stream=True)
            
            if file_response.status_code == 200:
                with open(file_name, 'wb') as f:
                    f.write(file_response.content)
                print(f"Downloaded: {file_name}")
            else:
                print(f"Failed to download {file_name}. Status code: {file_response.status_code}")