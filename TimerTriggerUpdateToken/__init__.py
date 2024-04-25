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

calllback_url ="https://bccok-oberoi-rfi-mail-time-trigger.azurewebsites.net/api/RefreshTokens?"
# calllback_url ="http://localhost:7071/api/RefreshTokens"


async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # Call the function to make the GET request
    await make_get_request()

async def make_get_request():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(calllback_url) as response:
                if response.status == 200:
                    data = await response.text()
                    print(data)  # Print or process the response data here
                else:
                    print(f"Request failed with status code {response.status}")
    except aiohttp.ClientError as e:
        print(f"Error: {e}")


