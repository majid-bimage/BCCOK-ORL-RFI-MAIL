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
import asyncio
app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

app.config['SECRET_KEY'] = 'your-secret-key'  # Change this to a secure secret key
app.config['MAIL_SERVER'] = 'smtp.office365.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = 'acc.support@bimageconsulting.in'
app.config['MAIL_PASSWORD'] = 'Bimage@Karan'    # os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = 'acc.support@bimageconsulting.in'

mail = Mail(app)

app.config['server'] = "bimageforge.database.windows.net"
app.config['database'] = "bimageforge"
app.config['username'] = "forge"
app.config['password'] = "BimageNow2020"
app.config['driver'] = 'ODBC Driver 18 for SQL Server'
app.config['conn_str'] = f"DRIVER={{{app.config['driver']}}};SERVER={app.config['server']};DATABASE={app.config['database']};UID={app.config['username']};PWD={app.config['password']}"

ForgeClientID = "mfnSk1lkE4MCuX5nn11AdKHsKdSkkEMv" 
# Define the connection string
connection_string = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=bimageforge.database.windows.net;DATABASE=bimageforge;UID=forge;PWD=BimageNow2020"

class Token3Legged:
    def __init__(self, refresh_token, access_token, expiry_datetime):
        self.RefreshToken3Legged = refresh_token
        self.AccessToken3Legged = access_token
        self.AccessToken3LeggedExpiredDateTime = expiry_datetime

# Initialize tk3 globally
tk3 = Token3Legged(refresh_token="your_refresh_token_here",
                   access_token="your_access_token_here",
                   expiry_datetime="2024-04-22 12:00:00")

async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    # logging.info(tk3.AccessToken3Legged)

    logging.info('Python timer trigger function ran at %s', utc_timestamp[:-9])
    current_time = f"{utc_timestamp[:-9]}Z"
    current_time = dt.strptime(current_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    # Subtract 10 years
    old_time = current_time - timedelta(days=365*10)
    # old_time = f"{old_time[:-9]}Z"
    # old_time = dt.strptime(old_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    # logging.info(old_time)

    # access_token = await get_access_token()
    
    tk3 = await get_secret_from_table()
    while tk3 == None:
        logging.info("tk3 none")
        await asyncio.sleep(15)

        tk3 = await get_secret_from_table()

    
    access_token = tk3.AccessToken3Legged
    logging.info(access_token)
    # await clear_table()
    hub_id = "b.eaa661b8-ab46-4745-a18f-588f4de4348b"
    projects2 = await get_projects(access_token, hub_id)
    logging.info(len(projects2))

    projects=[]
    
    for y in projects2:
        if 1==1:
            projectid = y['id'][2:]
            data = await get_project_by_id(access_token, projectid)
            if data:
                projects.append(y)

    # projects =[]
    # projectid = "00fe3509-e5b1-4b0c-b16b-dc9d09726e3c"
    totalrfi = 0
    tt = 0
    logging.info(f"no of projects  {len(projects)}")
    for y in projects:
        # if y['id'] == 'b.00fe3509-e5b1-4b0c-b16b-dc9d09726e3c':
            # logging.info(f" {y['id']} - {y['attributes']['extension']['data']['projectType']} - ")
            # if y['id'] == 'b.50e17f11-e8bb-4814-b251-295f1786e9f7':
            #     logging.info(y)
            
        projecttype = y['attributes']['extension']['data']['projectType']
        containerid = y['relationships']['rfis']['data']['id']

    # if 1==1:
        projectid = y['id'][2:]
        project_users = await get_project_users(access_token, projectid)
        # logging.info(project_users)

        projectname= y['attributes']['name'] 
        rfis = await get_rfis(access_token, containerid, old_time, current_time)
        # logging.info(rfis)
        logging.info(f"No of rfis {y['id']} --   {containerid} - {len(rfis)}")
        # projects = []
        totalrfi +=len(rfis)
        rfis= []

        for x in rfis:
            try:
            # if 1==1:
                # logging.info(x['dueDate'])
                # logging.info(x['customIdentifier'])
                if x['dueDate'] != None:
                    # Parse string to datetime object
                    v = dt.strptime(x['dueDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    
                else:
                    sample = '2222-07-20T13:18:04.000Z'
                    v = dt.strptime(sample, '%Y-%m-%dT%H:%M:%S.%fZ')
                # logging.info(f" {v} --- {current_time}")
                
                if x['status'] != "closed" and x['status'] != "draft" and v < current_time:


                    tt+=1

                    pending_users_list = []
                    reviewers_list = []

                    pending_names = ""
                    reviewer_names = ""

                    # if 1==0:
                    # duedate = x['dueDate']
                    creator_data = await get_user_data(access_token, projectid, x['createdBy'], project_users)

                    creator = " - "
                    if creator_data:
                        creator = creator_data['name']
                        # logging.info(f"creator : {creator}")
                    manager_data = await get_user_data(access_token, projectid, x['managerId'], project_users)

                    if manager_data:
                        # logging.info(manager_data)
                        manager = manager_data['name']
                    rfi_id = x['id']
                    rfi_details = await get_rfi_by_id(access_token, containerid, rfi_id)
                    # logging.info(rfi_details)
                    # rfi_details = False
                    if rfi_details:
                        if 'permittedActions' in rfi_details:
    
                            for data in rfi_details['permittedActions']['remainingReviewers']:
                                userid = data['id']
                                # users = [users_list['id'] for users_list in rfi_details['permittedActions']['remainingReviewers']]

                                # logging.info(data['id'])
                                pending_user_data = await get_user_data(access_token, projectid, userid, project_users)

                                if pending_user_data:
                                    # logging.info(pending_user_data)
                                    data1 = {
                                        "userid": pending_user_data['autodeskId'],
                                        "email": pending_user_data['email'],
                                        "name": pending_user_data['name']
                                    }
                                
                                    pending_names += f"{pending_user_data['name']}, "
                                    pending_users_list.append(data1)

                                # logging.info(f"{user_data['name']} - {user_data['email']}" )
                            # logging.info(rfi_details)`
                        if 'reviewers' in rfi_details:
                                
                            for x in rfi_details['reviewers']:
                                reviewers_data = await get_user_data(access_token, projectid, x['oxygenId'], project_users)
                                if reviewers_data:
                                    data1 = {
                                        "userid": reviewers_data['autodeskId'],
                                        "email": reviewers_data['email'],
                                        "name": reviewers_data['name']
                                    }
                                    
                                    reviewer_names += f"{reviewers_data['name']}, "
                                    reviewers_list.append(data1)
                    if pending_users_list or manager_data:
                        
                        await send_mail(rfi_details, pending_users_list, pending_names, reviewer_names, creator, manager_data, v, projectname, projecttype)
                        pass

            except Exception as ex:
                # pass
                logging.info(f" ----- {ex}")
    logging.info(totalrfi)
    logging.info(tt)

async def send_mail( data, users, pending_names, reviewer_names, creator, manager, duedate, projectname, projecttype):
    # logging.info(data['title'])
    # logging.info(users)
    # table = await create_table_rfi_mail_details()
    # logging.info(f"table - {table}")
    table = True
    mail_sent_count = 0 
    if table:
        # "avis@bimageconsulting.in",
        if projecttype == "BIM360":
            webview = f"https:///pm.b360.autodesk.com/projects/{data['projectId']}/rfis?preview={data['id']}"
        else:
            webview = f"https:/acc.autodesk.com/build/rfis/projects/{data['containerId']}?preview={data['id']}"
        subject = "RFI Due Reminder"
        a= 0
        if len(users) > 0:
            # logging.info(f"len of users {len(users)}")
            # logging.info(manager)
            if manager == False:
                manager_name = ' - '
            else:
                manager_name = manager['name']

            for x in users:
                # logging.info(x)
                recipient = [x['email']]
                try:
                    email_body = f"""                <!doctype html>  <head>                     <meta name="viewport" content="width=device-width" />
                    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />                        <style>                            body{{
                                font-family: sans-serif;                            }}                            table {{                                border-collapse: collapse;
                                margin-bottom: 20px;                            }}                            /* Table header styling */                            th {{
                            }}
                            /* Table cell styling */                            th {{                            padding: 6px;                            border: none;
                            text-align: left;                            }}                            td {{                            padding: 1rem 10x;                            border: none;
                            text-align: left;                            }}    /* Alternating row colors for better readability */    tr:nth-child(even) {{    }}
    /* Hover effect on rows */    tr:hover {{    }}    .view_button{{        background-color: #0696d7;        padding: 1%;        border: none;        color: #fff;        text-decoration:none;
        margin-button:2%;    }}    .light_blue_div{{        background-color: #daf0f9;        padding :2%;        text-align:center;    }}    .rfi_link{{        color:#32a9de;
        font-size:26px;    }}    .p1{{        font-size:22px;    }}    .first_div{{        padding: 5% 5%;        background-color: #edf0f2;    }}    .second_div{{        background-color: #fff;
           }}    .content_div{{        background-color: #fff;        padding: 2%;        border-bottom :1px solid;    }}        </style>        </head>        <body>    <table class="first_div" cellpadding="20" cellspacing="0" border="0" style="width:100%;">
            <tr>
    <td>&nbsp;</td>
                <td style="background-color:#fff;">
        <div class="second_div">
        <div class="content_div">
            <p class="light_blue_div">Your action is required</p>
            <p class="p1">{x['name']}, RFI #{data['customIdentifier']} is waiting for your action:</p>
            <a href="{webview}" class="rfi_link"> RFI #{data['customIdentifier']} {data['title']} </a>
        </div>
        <div class="content_div">
            <p class="p1"><b>Details</b></p>
            <table>
            <tr>
                <th>Project Name </th><th></th>
                <td> {projectname} </td>
            </tr>
            <tr>
                <th>Status</th><th></th>
                <td>{data['status'].capitalize()}</td> 
            </tr>
            <tr>
                <th>Ball in court</th><th></th>
                <td>{pending_names[:-2]}</td> 
            </tr>
            <tr>
                <th>Due date</th><th></th>
                    <td>{data['dueDate'][:-14]}</td> 
            </tr>

            </table>
        </div>
        <div class="content_div">
            <p class="p1"><b>Participants</b></p>
            <table>
            <tr>
                <th>Creator</th><th></th>
                <td>{creator}</td> 
            </tr>
            <tr>
                <th>Manager</th><th></th>
                <td>{manager_name}</td> 
            </tr>
            <tr>
                <th>Reviewers</th><th></th>
                <td>{reviewer_names[:-2]}</td> 
            </tr>

            </table>
        </div>
            <div class="content_div">
                <center>
                <a href="{webview}" class="view_button">
                View RFI </a>
                        
                    <p style="color: #848f95;">
                        Generated by BIMAGE CONSULTING.
                    </p>
                </center>
            </div>
                
                </div>
                </td>
                <td></td>

            </tr>
            </table>
                    </body></html>"""
                    with app.app_context():
                        message = Message(subject=subject, recipients=recipient, html=email_body)
                        # if a ==0:
                        mail.send(message)
                            # a +=1
                        recipient = ['majid.n@bimageconsulting.in']
                        message = Message(subject=subject, recipients=recipient, html=email_body)

                        mail.send(message)
                        mail_sent_count += 1
                        # logging.info(f"Email sent successfully to : {x['email']}")
                        # Example usage:
                        utc_timestamp = datetime.datetime.utcnow().replace(
                        tzinfo=datetime.timezone.utc).isoformat()
                        current_time = f"{utc_timestamp[:-9]}Z"
                        # insert_data(projectname, data['containerId'], data['title'], data['id'], data['customIdentifier'],  x['email'], x['userid'], current_time)
                        
                except Exception as e:
                    logging.error(f"Error sending email: {str(e)}")
            # logging.info(f"mails sent : {mail_sent_count}  - containerId : {data['containerId']}  -  rfiId : {data['id']}")

    #     else:
    #         recipient = [manager['email']]

    #         email_body = f"""
    #         <!doctype html>  <head> 
    #             <meta name="viewport" content="width=device-width" />
    #             <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    #                 <style>           body{{
    #         font-family: sans-serif;
    #     }}
    #     table {{
    #         border-collapse: collapse;
    #         margin-bottom: 20px;
    #     }}

    #     /* Table header styling */
    #     th {{
    #     }}

    #     /* Table cell styling */
    #     th {{
    #     padding: 6px;
    #     border: none;
    #     text-align: left;
    #     }}
    #     td {{
    #     padding: 1rem 10x;
    #     border: none;
    #     text-align: left;
    #     }}

    #     /* Alternating row colors for better readability */
    #     tr:nth-child(even) {{
    #     }}

    #     /* Hover effect on rows */
    #     tr:hover {{
    #     }}
    #     .view_button{{
    #         background-color: #0696d7;
    #         padding: 1%;
    #         border: none;
    #         color: #fff;
    #         text-decoration:none;
    #         margin-button:2%;
    #     }}
    #     .light_blue_div{{
    #         background-color: #daf0f9;
    #         padding :2%;
    #         text-align:center;
    #     }}
    #     .rfi_link{{
    #         color:#32a9de;
    #         font-size:26px;
    #     }}
    #     .p1{{
    #         font-size:22px;
    #     }}
    #     .first_div{{
    #         padding: 5% 5%;
    #         background-color: #edf0f2;
    #     }}
    #     .second_div{{
    #         background-color: #fff;
            

    #     }}
    #     .content_div{{
    #         background-color: #fff;
    #         padding: 2%;
    #         border-bottom :1px solid;
    #     }}

    #         </style>
    #         </head>
    #         <body>
    #     <table class="first_div" cellpadding="20" cellspacing="0" border="0" style="width:100%;">
    #             <tr>
    #     <td>&nbsp;</td>
    #         <td style="background-color:#fff;">
    # <div class="second_div">
    # <div class="content_div">
    #     <p class="light_blue_div">Your action is required</p>
    #     <p class="p1">{manager['name']}, RFI #{data['customIdentifier']} is waiting for your action:</p>
    #     <a href="{webview}" class="rfi_link"> RFI #{data['customIdentifier']} {data['title']} </a>
    # </div>
    # <div class="content_div">
    #     <p class="p1"><b>Details</b></p>
    #     <table>
    #     <tr>
    #         <th>Status<th>
    #         <td>{data['status'].capitalize()}</td> 
    #     </tr>
    #     <tr>
    #         <th>Ball in court<th>
    #         <td>{manager['name']}</td> 
    #     </tr>
    #     <tr>
    #         <th>Due date<th>
    #             <td>{data['dueDate'][:-14]}</td> 
    #     </tr>

    #     </table>
    # </div>
    # <div class="content_div">
    #     <p class="p1"><b>Participants</b></p>
    #     <table>
    #     <tr>
    #         <th>Creator<th>
    #         <td>{creator}</td> 
    #     </tr>
    #     <tr>
    #         <th>Manager<th>
    #         <td>{manager['name']}</td> 
    #     </tr>
    #     <tr>
    #         <th>Reviewers<th>
    #         <td>{reviewer_names[:-2]}</td> 
    #     </tr>

    #     </table>
    # </div>
    #     <div class="content_div">
    #         <center>
    #         <a href="{webview}" class="view_button">
    #         View RFI </a>
                    
    #             <p style="color: #848f95;">
    #                 Generated by BIMAGE CONSULTING.
    #             </p>
    #         </center>
    #     </div>
            
    #         </div>
    #         </td>
    #         <td></td>

    #     </tr>
    #     </table>
    #             </body></html>"""

    #         try:
    #             with app.app_context():
    #                 message = Message(subject=subject, recipients=recipient, html=email_body)
    #                 # if a ==0:
    #                 mail.send(message)
    #                     # a +=1
    #                 recipient = ['majid.n@bimageconsulting.in']
    #                 message = Message(subject=subject, recipients=recipient, html=email_body)

    #                 mail.send(message)

    #                 logging.info('Email sent successfully.')
    #                 # Example usage:
    #                 utc_timestamp = datetime.datetime.utcnow().replace(
    #                 tzinfo=datetime.timezone.utc).isoformat()
    #                 current_time = f"{utc_timestamp[:-9]}Z"
    #                 logging.info(manager)
    #                 await insert_data(projectname, data['containerId'], data['title'], data['id'], data['customIdentifier'],  manager['email'], manager['autodeskId'], current_time)
                    
    #         except Exception as e:
    #             logging.error(f"Error sending email: {str(e)}")

    else:
        logging.info("Table creation error")

async def get_user_data(access_token, projectid, _userid, project_users):
    # for x in project_users:

    #     if x['autodeskId'] == _userid and x['status'] == 'active':
    #         logging.info(f"{_userid} ---- {x['autodeskId']} 8888888888")

    #         return x
    # return False
    active_users = [x for x in project_users if x['autodeskId'] == _userid and x['status'] == 'active']
    if active_users:
        # logging.info(f"{_userid} ---- {active_users[0]['autodeskId']} 8888888888")
        return active_users[0]
    return False

    # try:

    #     url = f'https://developer.api.autodesk.com/construction/admin/v1/projects/{projectid}/users/{userid}'
    #     headers = {
    #         'Content-Type': 'application/json',
    #         'Authorization': 'Bearer '+ access_token
    #     }

    #     response = requests.get(url, headers=headers)
    #     if response.status_code == 200:
    #         if response.json()['status'] == "active":
    #             return response.json()
    #         else:
    #             # logging.info(f"{url} - user not active")
    #             return False
    #     else:
    #         # logging.info(f"Error: {response.status_code}, {response.text}")
            
    #         return False
    # except Exception as ex:
    #     logging.info(f" error in get_user_data {ex}")

async def get_rfi_by_id(access_token, projectid, rfiid):
    try:

        url = f'https://developer.api.autodesk.com/bim360/rfis/v2/containers/{projectid}/rfis/{rfiid}'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+ access_token
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:

            return response.json()
        else:
            logging.info(f"Error: {response.status_code}, {response.text}")
            return False
    except Exception as ex:
        logging.info(f" error in get_rfis {ex}")

async def get_rfis(access_token, projectid, old_time, current_time):
    try:
        res = []
        lim = 200
        off = 0
        total = 0
        url = f'https://developer.api.autodesk.com/bim360/rfis/v2/containers/{projectid}/rfis?limit={lim}&offset={off}&filter[status]=submitted&filter[status]=open&filter[status]=rejected&filter[status]=answered'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+ access_token
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            pagination = response.json()['pagination']
            # logging.info(pagination)
            total = pagination['totalResults']
            
        else:
            return f"Error: {response.status_code}, {response.text}"
        # logging.info(total)
    
        while (off<total):
            
            # logging.info("*" * 30)
            url = f'https://developer.api.autodesk.com/bim360/rfis/v2/containers/{projectid}/rfis?limit={lim}&offset={off}&filter[status]=submitted&filter[status]=open&filter[status]=rejected&filter[status]=answered'
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+ access_token
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                pagination = response.json()['pagination']
                off = pagination['offset'] + lim
                # total = pagination['totalResults']
                for x in response.json()['results']:
                    res.append(x)
                # return res
            else:
                return f"Error: {response.status_code}, {response.text}"
        return res
    except Exception as ex:
        logging.info(f" error in get_rfis {ex}")

async def get_access_token():
    # --------------------------------------------------------------------------------------------------
            
     # Define the connection string
    conn_str = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=bimageforge.database.windows.net;DATABASE=bimageforge;UID=forge;PWD=BimageNow2020'

    # Connect to the SQL Server database
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
        
    cursor.execute(" SELECT access_token FROM [BCCOK].[AccLoginTokens] WHERE expiry > GETDATE() AND company = 'oberoi' AND purpose = 'email' ")
    access_token = ''
    for r in cursor:    

        access_token = r[0]
        logging.info(access_token)
    
    if access_token == '':
        cursor.execute("SELECT headers FROM [BCCOK].[AccLoginTokens] WHERE company = 'oberoi' AND purpose = 'email'")
        # Fetch the results
        results = cursor.fetchall()

        for r in results:
            
            headers = json.loads(r[0])  
            logging.info(headers)
            
            rs = requests.get('https://login.acc.autodesk.com/api/v1/authentication/refresh?currentUrl=https%3A%2F%2Facc.autodesk.com%2Fprojects', headers=headers, data='')
            logging.info(rs.text) 
            js = json.loads(rs.text)
            access_token = js['accessToken']
            # logging.info(access_token)

            expiry = js['expiresAt']
            cursor.execute(f"UPDATE [BCCOK].[AccLoginTokens] SET access_token = '{access_token}' , expiry = '{expiry}' WHERE company = 'oberoi' AND purpose = 'email'")
            conn.commit()
    cursor.close()
    conn.close()

    return access_token
    # --------------------------------------------------------------------------------------------------

async def create_table_rfi_mail_details():
    try:
        # Connect to your database
        conn = pyodbc.connect(app.config['conn_str'])

        # Create a cursor object
        cursor = conn.cursor()
        query = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'BCCOK' AND TABLE_NAME = 'OberoiRfiMailList'
        """

        cursor.execute(query)
        table_exists = cursor.fetchone()

        if not table_exists:
            # Define the SQL query to create the table if not exists
            create_table_query = """
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'BCCOK.OberoiRfiMailList')
                BEGIN
                    CREATE TABLE [BCCOK].[OberoiRfiMailList] (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        ProjectName VARCHAR(MAX),
                        ProjectId VARCHAR(MAX),
                        RFIName VARCHAR(MAX),
                        RFIId  VARCHAR(MAX),
                        RFINumber VARCHAR(MAX),
                        UserEmail VARCHAR(MAX),
                        UserID VARCHAR(MAX),
                        MailSentTime VARCHAR(MAX)
                    );
                END;
            """

            # Execute the SQL query
            cursor.execute(create_table_query)
             # Commit the changes
            conn.commit()

            # Close the connection
            conn.close()
            # cursor.close()
            # logging.info("table created")
            return True

        else:
            # logging.info("Table already exists")
            # Commit the changes
            conn.commit()

            # Close the connection
            conn.close()
            # cursor.close()
            return True

        

    except Exception as ex :
        logging.info(f"Error in table creation - {ex}")
        return False
    
def insert_data(project_name, project_id, rfi_name, rfi_id, rfi_number, user_email, user_id, mail_sent_time):
    try:
        # Connect to your database
        conn = pyodbc.connect(app.config['conn_str'])

        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL query to insert data into the table
        insert_query = '''
        INSERT INTO BCCOK.OberoiRfiMailList (ProjectName, ProjectId, RFIName, RFIId, RFINumber, UserEmail, UserID, MailSentTime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''

        # Execute the SQL query with parameters
        cursor.execute(insert_query, (project_name, project_id, rfi_name, rfi_id, rfi_number, user_email, user_id, mail_sent_time))

        # Commit the transaction
        conn.commit()

        logging.info("Data inserted successfully.")
        return True

    except Exception as e:
        logging.info(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        conn.close()
        # cursor.close()

async def get_projects(access_token, hub_id):
    
    try:

        url = f'https://developer.api.autodesk.com/project/v1/hubs/{hub_id}/projects'
        # logging.info(url)
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+ access_token
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:

            return response.json()['data']
        else:
            return f"Error: {response.status_code}, {response.text}"
    except Exception as ex:
        logging.info(f" error in get_rfis {ex}")

async def clear_table():
    # Establish a connection to your database
    conn = pyodbc.connect(app.config['conn_str'])

    # Create a cursor object
    cursor = conn.cursor()

    # Define your SQL query
    sql_query = "DELETE FROM [BCCOK].[OberoiRfiMailList]"

    try:
        # Execute the query
        cursor.execute(sql_query)
        
        # Commit the transaction
        conn.commit()
        
        logging.info("Deletion successful")
        cursor.close()
        conn.close()
    except Exception as e:
        # Rollback the transaction if an error occurs
        conn.rollback()
        logging.info("Deletion failed:", e) 

async def get_project_by_id(access_token, project_id):
    try:
        url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{project_id}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+ access_token
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            project = response.json()
            # if project['status']
            current_date_iso8601 = dt.now().date().isoformat()
            if project['endDate'] == None and project['status'] == "active":
                # logging.info(f"{project['status']} ---{project['endDate']}")

                return True
            elif project['status'] == "active" and project['endDate'] >= current_date_iso8601:
                # logging.info(f"{project['status']} ---{project['endDate']}")
                # return response.json()
                return True
        else:
            logging.info( f"Error: {response.status_code}, {response.text}")
            return False
    except Exception as ex:
        logging.info(f"error in get_project_by_id --- {ex}")
        return False

async def get_project_users(access_token, projectId):
    try:
        res = []
        lim = 200
        off = 0
        total = 0
        url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{projectId}/users?limit={lim}&offset={off}"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+ access_token
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            pagination = response.json()['pagination']
            # logging.info(pagination)
            total = pagination['totalResults']
            
        else:
            return f"Error: {response.status_code}, {response.text}"
        # logging.info(total)
    
        while (off<total):
            
            # logging.info("*" * 30)
            url = f"https://developer.api.autodesk.com/construction/admin/v1/projects/{projectId}/users?limit={lim}&offset={off}"

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer '+ access_token
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                pagination = response.json()['pagination']
                off = pagination['offset'] + lim
                total = pagination['totalResults']
                for x in response.json()['results']:
                    # logging.info(x)
                    res.append(x)
                # return res
            else:
                return f"Error: {response.status_code}, {response.text}"
        return res
    except Exception as ex:
        logging.info(f" error in get_rfis {ex}")





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