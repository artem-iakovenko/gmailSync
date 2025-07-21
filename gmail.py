from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from helper import array_splitter, parts_parser, parse_gmail_headers
import time
import json
import base64
from config import GMAIL_SCOPES


class GoogleMail:
    def __init__(self, email, tokens, cursor_date):
        self.email = email
        self.tokens = tokens
        self.creds = None
        self.service = None
        self.start_date = cursor_date
        self.page_results = 500
        self.mail_messages = []
        self.filtered_messages = []
        self.full_messages = []
        self.prepared_messages = []
        self.messages_for_bq = []
        self.files_to_upload = []
        self.status_tracking = {"success": [], "failure": []}

    def get_service(self):
        if not self.creds or not self.creds.valid:
            try:
                self.creds = Credentials.from_authorized_user_info(self.tokens, GMAIL_SCOPES)
                self.creds.refresh(Request())
                self.service = build('gmail', 'v1', credentials=self.creds)
            except:
                print("Unable to log in")

    def get_messages(self):
        query = f'in:anywhere AND after:{self.start_date}'
        page_token = None
        while True:
            self.get_service()
            if not self.service:
                return
            page_messages_response = self.service.users().messages().list(
                userId='me',
                q=query,
                maxResults=self.page_results,
                pageToken=page_token
            ).execute()
            page_messages = page_messages_response['messages']
            self.mail_messages.extend(page_messages)
            try:
                page_token = page_messages_response["nextPageToken"]
            except KeyError:
                page_token = None
            if not page_token:
                break
        print(f"TOTAL MESSAGES FOUND AFTER {self.start_date}: {len(self.mail_messages)}")

    def filter_messages(self, db_message_ids):
        for message in self.mail_messages:
            if message['id'] not in db_message_ids:
                self.filtered_messages.append(message)
        print(f"TOTAL MESSAGES AFTER FILTERING: {len(self.filtered_messages)}")

    def get_batch_message_details(self):
        for msg in self.filtered_messages:
            message_details = self.service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            success_check = True if 'id' in message_details else False
            print(f"\t\tGetting Message {msg['id']} - Status: {success_check}")
            if success_check:
                self.full_messages.append(message_details)
                self.status_tracking['success'].append(msg['id'])
            else:
                self.status_tracking['failure'].append(msg['id'])

    # def get_batch_message_details(self):
    #     requests_counter = 0
    #     for splitted_message in array_splitter(self.filtered_messages, 100):
    #         self.get_service()
    #         batch = self.service.new_batch_http_request()
    #         for message in splitted_message:
    #             requests_counter += 1
    #             print(f"\t\tGetting Message: {requests_counter} of {len(self.filtered_messages)}")
    #             batch.add(self.service.users().messages().get(userId='me', id=message['id'], format='full'))
    #         batch.execute()
    #         time.sleep(6)
    #
    #         for request_id in batch._order:
    #             resp, content = batch._responses[request_id]
    #             message_content = json.loads(content)
    #             self.full_messages.append(message_content)

    def save_files(self, message_id, part):
        try:
            attachment_name, attachment_headers, is_attached_file = part['filename'], part['headers'], False
        except KeyError:
            return None
        try:
            part_id = float(part['partId'])
        except:
            part_id = 0.1

        for attachment_header in attachment_headers:
            if attachment_header['name'] == 'Content-Disposition':
                content_description = attachment_header['value']
                if 'attachment' in content_description and part_id >= 1:
                    is_attached_file = True
                break
        file_name = f"{message_id}-{attachment_name}"
        file_path = f"files/{file_name}"

        if is_attached_file:
            try:
                attachment_details = self.service.users().messages().attachments().get(userId='me', messageId=message_id,
                                                                              id=part['body']['attachmentId']).execute()
                attachment_data, attachment_size = attachment_details['data'], attachment_details['size']
                attachment_size_kb = float(attachment_size) / 1000
                attachment_size_mb = attachment_size_kb / 1000
            except:
                return None
            if attachment_size_mb > 10 or attachment_size_kb < 10:
                print('\t\t\t- Not Relevant Size')
                return None
            print(f"\t\t\t- {attachment_name}, {attachment_size_mb} MB")
            file_data = base64.urlsafe_b64decode(attachment_data.encode('UTF-8'))
            self.files_to_upload.append({
                "message_id": message_id,
                "attachment_name": attachment_name,
                "file_name": file_name,
                "file_path": file_path,
                "file_data": file_data
            })
        print('\t\t\t- Not Relevant Attachment Type or Part ID')
        return None

    def messages_to_bq_format(self):
        message_counter = 1
        for message in self.full_messages:
            try:
                message_id, thread_id, payload, at_c = message['id'], message['threadId'], message['payload'], 0
                headers, parsed_parts = payload['headers'], parts_parser([payload])
                parsed_headers = parse_gmail_headers(headers)
                message_from, message_to, message_cc, message_date, message_subject = parsed_headers[0], parsed_headers[1], parsed_headers[2], parsed_headers[3], parsed_headers[4]
                message_html, message_text, body_type, message_attachments = "", "", "", []
            except Exception as e:
                print(e)
                continue
            for part in parsed_parts:
                part_mime, part_body = part['mimeType'], part['body']
                if 'attachmentId' in part_body:
                    self.save_files(message_id, part)
                try:
                    if part_mime == 'text/html':
                        message_html = part_body['data']
                    elif part_mime == 'text/plain':
                        message_text = part_body['data']
                except KeyError:
                    continue

            if message_html:
                message_body, body_type = str(base64.urlsafe_b64decode(message_html).decode("utf-8")), 'html'
            elif message_text:
                message_body, body_type = str(base64.urlsafe_b64decode(message_text).decode("utf-8")), 'text'
            else:
                message_body, body_type = "", ""
            if not message_date:
                print("Message Date is Empty")
                continue
            # else:
            #     print(message_date)
            self.messages_for_bq.append(
                {
                    "messageId": message_id,
                    "subject": message_subject,
                    "from": message_from,
                    "to": message_to,
                    "cc": message_cc,
                    "date": message_date,
                    "bodyText": message_body,
                    "threadId": thread_id,
                    "messageOwner": self.email,
                    "bodyType": body_type
                }
            )
            print(f"\t\t{message_counter}. {body_type} message: {message_id} - from {message_from} to {message_to}")
            message_counter += 1