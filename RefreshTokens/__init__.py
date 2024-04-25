from asyncio import log
import base64
import datetime
from datetime import datetime as dt
import logging
import pymssql
import requests
import json
import azure.functions as func
import pyodbc
from flask import Flask
from flask_mail import Mail, Message
from datetime import timedelta
import aiohttp
import asyncio
from datetime import timedelta

class Token3Legged:
    def __init__(self, refresh_token, access_token, expiry_datetime):
        self.RefreshToken3Legged = refresh_token
        self.AccessToken3Legged = access_token
        self.AccessToken3LeggedExpiredDateTime = expiry_datetime

# Initialize tk3 globally
tk3 = Token3Legged(refresh_token="your_refresh_token_here",
                        access_token="your_access_token_here",
                        expiry_datetime="2024-04-22 12:00:00")
ForgeClientID = "mfnSk1lkE4MCuX5nn11AdKHsKdSkkEMv" 
ForgeClientSecret = "PpKJ7fzgkepmpxw4"
# CallBackURL = "http://localhost:7071/api/RefreshTokens"
CallBackURL = "https://bccok-oberoi-rfi-mail-time-trigger.azurewebsites.net/api/RefreshTokens"
scope = "data:read account:read"
# Define the connection string
connection_string = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=bimageforge.database.windows.net;DATABASE=bimageforge;UID=forge;PWD=BimageNow2020"

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    name = req.params.get('name')
    
    # Assuming tk3 is obtained from GetSecretFromTable
    tk3 = await get_secret_from_table()

    if tk3 is None:
        AuthCode = req.params.get("code")
        if not AuthCode:
            url = f"https://developer.api.autodesk.com/authentication/v1/authorize?response_type=code&client_id={ForgeClientID}&redirect_uri={CallBackURL}&scope={scope}"
            logging.info(url)
            # redirectResult = RedirectResult(url)
            # return redirectResult
        else:
            logging.info(AuthCode)
            tk3a = await update_3_legged_token_using_auth_code(AuthCode, log)
            await insert_tokens(tk3a)
            responseMessage = "This HTTP triggered function executed successfully."
            return func.HttpResponse(responseMessage) # type: ignore
    else:
        AccessToken3Legged = tk3.AccessToken3Legged
        AccessToken3LeggedExpiredDateTime = tk3.AccessToken3LeggedExpiredDateTime
        currentTime = dt.now() + datetime.timedelta(minutes=15)
        logging.info(AccessToken3LeggedExpiredDateTime)
        logging.info(AccessToken3Legged)
        logging.info(currentTime)
        if currentTime > AccessToken3LeggedExpiredDateTime:
            logging.info("Current time is greater than the specified time.")
            refresh_token = tk3.RefreshToken3Legged
            tk3aa = await refresh_tokens(refresh_token)
            
            AccessToken3Legged = tk3aa.AccessToken3Legged
            logging.info(AccessToken3Legged)
            logging.info(tk3aa.AccessToken3LeggedExpiredDateTime)
        elif currentTime == AccessToken3LeggedExpiredDateTime:
            logging.info("Current time is equal to the specified time.")
        else:
            logging.info("Current time is less than the specified time.")
        return func.HttpResponse( "This HTTP triggered function executed successfully.",status_code=200)

async def refresh_tokens(refresh_token):
    try:
        logging.info(refresh_token)
        async with aiohttp.ClientSession() as session:
            url = "https://developer.api.autodesk.com/authentication/v2/token"
            client_credentials = f"{ForgeClientID}:{ForgeClientSecret}"
            client_credentials_b64 = base64.b64encode(client_credentials.encode()).decode()
            headers = {
                "Authorization": f"Basic {client_credentials_b64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code  == 200:
                logging.info(response.status_code)
                logging.info("success token refresh")


                data = response.json()

                tk3 = Token3Legged(refresh_token="your_refresh_token_here",
                        access_token="your_access_token_here",
                        expiry_datetime="2024-04-22 12:00:00")
                
                tk3.AccessToken3Legged = data["access_token"]
                tk3.RefreshToken3Legged = data["refresh_token"]
                tk3.AccessToken3LeggedExpiredDateTime = dt.now() + timedelta(seconds=int(data["expires_in"]))
                update_status = await update_refresh_token(tk3)
                logging.info(f"{update_status} Update success")
                return tk3
            else:
                logging.info("reresh token fail")
                logging.info(response.status_code )
                logging.info(response.content )

   
                body = response.text()
                logging.info(body)
                logging.info(response.status_code )
                return None
    except Exception as ex:
        logging.info(f"Error in refresh token : {ex}")
        return tk3
async def update_refresh_token(data):
    try:
        # Connection string
        connection_string = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=bimageforge.database.windows.net;DATABASE=bimageforge;UID=forge;PWD=BimageNow2020"
        
        # Update query
        update_sql = """
            UPDATE [BCCOK].[RefreshTokens]
            SET [expiry] = ?,
                [tm8] = ?,
                [expiry8] = ?,
                [access_token] = ?,
                [refreshtoken] = ?
            WHERE [client_id] = ?
        """
        
        # Execute the update
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                # Execute the update query with parameters
                cursor.execute(update_sql, (data.AccessToken3LeggedExpiredDateTime, data.AccessToken3LeggedExpiredDateTime,
                                             data.AccessToken3LeggedExpiredDateTime, data.AccessToken3Legged,
                                             data.RefreshToken3Legged, ForgeClientID))
                # Commit the transaction
                connection.commit()
                # Get the number of rows affected
                logging.info("*" * 30)
                rows_affected = cursor.rowcount
                logging.info(f"{rows_affected} row(s) updated.")
                connection.close()
                cursor.close()
                return True
    except Exception as ex:
        logging.info("Error in update_refresh_token :", ex)


async def get_secret_from_table():
    try:
        
        # SQL query
        tsql = f"SELECT TOP 1 refreshtoken, expiry, access_token FROM BCCOK.RefreshTokens WHERE refreshtoken IS NOT NULL AND client_id = '{ForgeClientID}' ORDER BY tm8 DESC"
        
        # Execute query
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tsql)
                row = cursor.fetchone()
                if row:
                    tk3 = Token3Legged(refresh_token="your_refresh_token_here",
                        access_token="your_access_token_here",
                        expiry_datetime="2024-04-22 12:00:00")
                    
                    tk3.RefreshToken3Legged = row[0]
                    tk3.AccessToken3LeggedExpiredDateTime = row[1]
                    tk3.AccessToken3Legged = row[2]
                    return tk3
                else:
                    return None
    except Exception as ex:
        logging.info(f"Error: {ex}")
        return None
    
async def update_3_legged_token_using_auth_code(AuthCode, log):
    try:
        logging.info("Getting New AccessToken using AuthCode")

        async with aiohttp.ClientSession() as session:
            url = "https://developer.api.autodesk.com/authentication/v2/token"
            client_credentials = f"{ForgeClientID}:{ForgeClientSecret}"
            client_credentials_b64 = base64.b64encode(client_credentials.encode()).decode()
            headers = {
                "Authorization": f"Basic {client_credentials_b64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            payload = {
                "grant_type": "authorization_code",
                "code": AuthCode,
                "redirect_uri": CallBackURL
            }
            async with session.post(url, headers=headers, data=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    tk3 = Token3Legged(refresh_token="your_refresh_token_here",
                        access_token="your_access_token_here",
                        expiry_datetime="2024-04-22 12:00:00")
                    # tk3 = Token3Legged()
                    tk3.AccessToken3Legged = data["access_token"]
                    tk3.RefreshToken3Legged = data["refresh_token"]
                    tk3.AccessToken3LeggedExpiredDateTime = dt.now() + timedelta(seconds=int(data["expires_in"]))

                    return tk3
                else:
                    body = await response.text()
                    logging.info(body)
                    logging.info(response.status)
                    return None
    except Exception as ex:
        logging.info(ex)
        return None
    

async def insert_tokens(tk3):
    try:
        
        
        # Establish a connection
        with pyodbc.connect(connection_string) as connection:
            # Define the SQL INSERT statement
            sql = """
                INSERT INTO [BCCOK].[RefreshTokens] ([app], [client_id], [refreshtoken], [expiry], [tm8], [expiry8], [access_token], [msg])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Create a cursor
            cursor = connection.cursor()
            
            # Execute the SQL statement with parameters
            cursor.execute(sql, ('ORL', ForgeClientID, tk3.RefreshToken3Legged, tk3.AccessToken3LeggedExpiredDateTime, 
                                 tk3.AccessToken3LeggedExpiredDateTime, tk3.AccessToken3LeggedExpiredDateTime, 
                                 tk3.AccessToken3Legged, 'ORL RFI MAIL'))
            
            # Commit the transaction
            connection.commit()
            
            # Check if any rows were affected
            if cursor.rowcount > 0:
                logging.info("Data inserted successfully.")
                return "success"
            else:
                logging.info("No rows were inserted.")
                return "error"
    except Exception as ex:
        logging.info(ex)
        return None
    

