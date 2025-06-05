import pandas as pd
import requests
import io
import gspread
from google.oauth2 import service_account
from gspread_dataframe import set_with_dataframe
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

def download_csv_from_drive(file_id, creds):
    drive_service = build("drive", "v3", credentials=creds)
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    df = pd.read_csv(fh, sep=";")
    return df

def download_json_feed(token):
    headers = {"x-access-token": token}
    url = "http://partners.nrp.net.ua/api/vendors/v1/feed"
    response = requests.get(url, headers=headers)
    data = response.json()
    df = pd.json_normalize(data)
    return df

def match_and_clean(csv_df, json_df):
    df = csv_df.merge(json_df, how="left", left_on="merchant_offer_code", right_on="Code")
    df["price"] = df.apply(lambda row: 0 if row.get("Amount", 1) == 0 else row.get("Price", 0), axis=1)
    df["amount"] = df.get("Amount", 0).fillna(0)
    df["max_pay_in_parts"] = df.get("Max Pay in Parts", 0).fillna(0)
    return df[["product_id", "merchant_offer_code", "price", "amount", "max_pay_in_parts"]]

def main(request):
    FILE_ID = "1Q032299MGwURcmUGhthxmvBRYwXZPQFP"
    TOKEN = "029d4bbe25387369fad9831eae825acd"
    SPREADSHEET_ID = "1YhkZIIzSy--coI2rGs1nqtNf5pND3oiYjTaXefrOeko"
    SHEET_NAME = "moyo"
    CREDENTIALS_JSON = "credentials.json"

    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_JSON,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    df_csv = download_csv_from_drive(FILE_ID, creds)
    df_json = download_json_feed(TOKEN)
    df_final = match_and_clean(df_csv, df_json)

    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    sheet.clear()
    print(df_final.head())  # додано для виводу перших 5 рядків
    set_with_dataframe(sheet, df_final)

    return "✅ Данні оновлено в Google Sheets!"
