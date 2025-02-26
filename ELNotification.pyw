"""Script to send notifications to case managers that they have an EL student on their caseload on the first day of each term.

https://github.com/Philip-Greyson/D118-EL-Case-Manager-Notifications

Needs the google-api-python-client, google-auth-httplib2 and the google-auth-oauthlib:
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
also needs oracledb: pip install oracledb --upgrade
"""

import base64
import os
from datetime import datetime
from email.message import EmailMessage

import oracledb  # needed for connection to PowerSchool server (oracle database)
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# setup db connection
DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to
print(f'DBUG: Database Username: {DB_UN} |Password: {DB_PW} |Server: {DB_CS}')  # debug so we can see where oracle is trying to connect to/with

# Google API Scopes that will be used. If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.compose']

SEND_REGARDLESS_OF_DATE = False  # boolean that allows the emails to be sent regardless if it is not the first day of the term

if __name__ == '__main__':
    with open('el_notifications_log.txt', 'w') as log:
        startTime = datetime.now()
        startTime = startTime.strftime('%H:%M:%S')
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        service = build('gmail', 'v1', credentials=creds)  # create the Google API service with just gmail functionality

        # create the connecton to the PowerSchool database
        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:
            with con.cursor() as cur:  # start an entry cursor
                today = datetime.now()
                today = today.replace(hour=0, minute=0, second=0, microsecond=0)  # set the time to midnight, as all log entries have just midnight time stamps besides the date.
                print(f'DBUG: Todays datecode without time is {today}') 
                print(f'DBUG: Todays datecode without time is {today}', file=log)


                # first we need to find all school ids, ignoring schools that are exluded from state reporting
                cur.execute('SELECT school_number, name FROM schools WHERE state_excludefromreporting = 0')
                schools = cur.fetchall()
                for school in schools:
                    yearID = None  # reset the year ID to null for each school at the start so that if we dont find a year we skip trying to find sections
                    yearExpression = None  # we will keep the common language year expression in this term, reset it each school
                    schoolID = school[0]
                    emailsSentForSchool = False  # flag to track whether we have already sent emails for this school. This will prevent multiple batches being sent when quarters line up with semesters.
                    print(f'DBUG: Found school with id {schoolID}: {school[1]}')
                    print(f'DBUG: Found school with id {schoolID}: {school[1]}', file=log)
                    # next find the current termyear and terms in that year
                    cur.execute('SELECT firstday, lastday, yearid FROM terms WHERE schoolid = :school AND isyearrec = 1', school=schoolID)  # search for only year terms to narrow our list
                    years = cur.fetchall()
                    for year in years:
                        if (year[0] < today) and (year[1] > today):
                            yearID = year[2]  # store that terms yearcode into yearID so we can use it to search for all terms this year
                            print(f'DBUG: Found current year code for school {schoolID} to be {yearID}')
                            print(f'DBUG: Found current year code for school {schoolID} to be {yearID}', file=log)
                            # now find all terms that are in the termyear we found above
                            if yearID:
                                cur.execute('SELECT id, firstday, abbreviation FROM terms WHERE schoolid = :school AND yearid = :year AND isyearrec = 0 ORDER BY id', school=schoolID, year=yearID)
                                terms = cur.fetchall()
                                for term in terms:
                                    try:
                                        termID = term[0]
                                        termStart = term[1].replace(hour=0, minute=0, second=0, microsecond=0)  # set the time to midnight so we only have to worry about the date
                                        termName = str(term[2])
                                        print(f'DBUG: Found term {termName} with ID {termID} that starts {termStart}')
                                        print(f'DBUG: Found term {termName} with ID {termID} that starts {termStart}', file=log)
                                        print(f'DBUG: Todays date of {today} is {('EQUAL' if today == termStart else 'NOT EQUAL')} to the term start date of {termStart}')
                                        print(f'DBUG: Todays date of {today} is {('EQUAL' if today == termStart else 'NOT EQUAL')} to the term start date of {termStart}', file=log)

                                        if ((today == termStart) or SEND_REGARDLESS_OF_DATE) and not emailsSentForSchool:  # if it is the first day of a term, or we are ignoring it and sending anyways. But check if we have already sent so we dont have to do extra work past the first matching term of each building
                                            print('INFO: It is the first day of a term or the flag is set to send regardless')
                                            print('INFO: It is the first day of a term or the flag is set to send regardless', file=log)
                                            try:
                                                caseManagerStudents = {}  # create a dictionary that will have the case manager emails as keys and a list of students as its values
                                                cur.execute('SELECT stu.student_number, stu.id, stu.first_name, stu.last_name, ext.casemanager, ext.case_manager_email, il.lep FROM STUDENTS stu LEFT JOIN u_def_ext_students0 ext ON stu.dcid = ext.studentsdcid LEFT JOIN s_il_stu_x il ON stu.dcid = il.studentsdcid WHERE ext.casemanager IS NOT NULL AND stu.enroll_status = 0 AND stu.schoolid = :school', school=schoolID)
                                                students = cur.fetchall()
                                                for student in students:  # go through each student one at a time
                                                    try:
                                                        # print(student)  # debug
                                                        stuNum = int(student[0])
                                                        stuID = int(student[1])
                                                        firstName = str(student[2])
                                                        lastName = str(student[3])
                                                        stuInfo = f'{firstName.title()} {lastName.title()} - {stuNum}'  # construct the student info string that contains their name and ID number
                                                        caseManager = str(student[4])
                                                        caseManagerEmail = str(student[5]) if student[5] else None
                                                        elLearner = True if student[6] == 1 else False

                                                        if elLearner:
                                                            if caseManagerEmail:
                                                                try:
                                                                    studentList = caseManagerStudents.get(caseManagerEmail)  # get the student list associated with the case manager. If the case manager is not already in the dict, it will return None
                                                                    if studentList:  # if the case manager email already exists in the dictionary, need to add the student to the list
                                                                        studentList.append(stuInfo)
                                                                        caseManagerStudents.update({caseManagerEmail: studentList})
                                                                        print(f'DBUG: Added {stuInfo} to the case load of {caseManager} - {caseManagerEmail}')
                                                                        print(f'DBUG: Added {stuInfo} to the case load of {caseManager} - {caseManagerEmail}', file=log)
                                                                    else:  # the case manager is not a member of the dict yet, need to add them and the first student to their list
                                                                        initialList = [stuInfo]
                                                                        print(f'DBUG: {caseManager} - {caseManagerEmail} is not part of the dict yet, will add them with the first student of {stuInfo}')
                                                                        print(f'DBUG: {caseManager} - {caseManagerEmail} is not part of the dict yet, will add them with the first student of {stuInfo}', file=log)
                                                                        caseManagerStudents.update({caseManagerEmail: initialList})
                                                                except Exception as er:
                                                                    print(f'ERROR while checking or adding {stuNum} to case manager {caseManagerEmail} dictionary: {er}')
                                                                    print(f'ERROR while checking or adding {stuNum} to case manager {caseManagerEmail} dictionary: {er}', file=log)
                                                    except Exception as er:
                                                        print(f'ERROR while doing initial processing of student {student[0]}: {er}')
                                                        print(f'ERROR while doing initial processing of student {student[0]}: {er}', file=log)
                                                try:
                                                    # send the emails
                                                    for manager in caseManagerStudents.keys():
                                                        print(f'INFO: The caseload for {manager} includes the following EL students, will send an email:', file=log)
                                                        students = caseManagerStudents.get(manager)
                                                        studentsOnCaseLoad = ''
                                                        for student in students:
                                                            studentsOnCaseLoad += f'{student}\n'
                                                            # print(student, file=log)
                                                        print(studentsOnCaseLoad, file=log)
                                                        try:
                                                            mime_message = EmailMessage()  # create an email message object
                                                            # define headers
                                                            mime_message['To'] = manager  # who the email gets sent to
                                                            mime_message['Subject'] = 'Reminder: You have EL students on your caseload'  # subject line of the email
                                                            mime_message.set_content(f'This is a notification that you have the following EL students on your caseload:\n{studentsOnCaseLoad}\n\nIf you feel this information is incorrect please reach out to the special services office.')  # body of the email
                                                            # encoded message
                                                            encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
                                                            create_message = {'raw': encoded_message}
                                                            send_message = (service.users().messages().send(userId="me", body=create_message).execute())
                                                            print(f'DBUG: Email sent, message ID: {send_message["id"]}') # print out resulting message Id
                                                            print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
                                                            
                                                        except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
                                                            status = er.status_code
                                                            details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
                                                            print(f'ERROR {status} from Google API while sending email to {manager} to notify them of their EL students: {details["message"]}. Reason: {details["reason"]}')
                                                            print(f'ERROR {status} from Google API while sending email to {manager} to notify them of their EL students: {details["message"]}. Reason: {details["reason"]}', file=log)
                                                        except Exception as er:
                                                            print(f'ERROR while trying to send email to {manager} to notify them of their EL students: {er}')
                                                            print(f'ERROR while trying to send email to {manager} to notify them of their EL students: {er}', file=log)
                                                    emailsSentForSchool = True
                                                except Exception as er:
                                                    print(f'ERROR while preparing EL student list to send in email to {manager}: {er}')
                                                    print(f'ERROR while preparing EL student list to send in email to {manager}: {er}', file=log)
                                            except Exception as er:
                                                print(f'ERROR while processing terms for building {schoolID} in year ID {yearID}: {er}')
                                                print(f'ERROR while processing terms for building {schoolID} in year ID {yearID}: {er}', file=log)
                                    except Exception as er:
                                        print(f'ERROR while retrieving student information in building {schoolID}: {er}')
                                        print(f'ERROR while retrieving student information in building {schoolID}: {er}', file=log)
        endTime = datetime.now()
        endTime = endTime.strftime('%H:%M:%S')
        print(f'INFO: Execution ended at {endTime}')
        print(f'INFO: Execution ended at {endTime}', file=log)
