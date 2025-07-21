import time
from helper import array_splitter
from database import BigQuery
from gmail import GoogleMail
import json
from gdrive import GoogleDrive
from datetime import datetime
from dateutil.relativedelta import relativedelta

EMAILS = ['kira@kitrum.com']


def main():
    # GET ALL FILES FROM SHARED GOOGLE DRIVE
    current_date = datetime.today()
    cursor_date = (current_date - relativedelta(months=1)).strftime('%Y-%m-%d')
    results = {'sync_results': {}}
    now = datetime.now()
    results['sync_datetime'] = now.strftime("%Y-%m-%dT%H:%M:%S")
    gdrive_handler = GoogleDrive()
    gdrive_handler.list_files()

    print(f"Finished Getting Files from Google Drive at {datetime.now()}")
    print(len(gdrive_handler.drive_files), " files found in Shared Drive")
    bigquery_handler = BigQuery()
    query = "SELECT email, tokens FROM `kitrum-cloud.gmail.credentials` where isValid = True"
    gmail_accounts = bigquery_handler.get_from_bigquery(query)

    for account in gmail_accounts:
        account_email = account['email']
        if 'iakovenko' in account_email:
            continue
        print("=" * 50)
        print(f"{account_email}")
        time.sleep(2)
        try:
            account_tokens = json.loads(account['tokens'])
            gmail_handler = GoogleMail(account_email, account_tokens, cursor_date)
            gmail_handler.get_messages()
            login_status = True if gmail_handler.service else False
            if not login_status:
                sync_stats = {
                    "login_status": login_status,
                    "new_messages": 0,
                    "downloaded_messages": 0,
                    "errors": []
                }
                results['sync_results'][account_email] = sync_stats
                continue

            # GET ALREADY EXISTED MESSAGES IN DATABASE BY CURRENT OWNER
            query = f"SELECT `messageId` FROM `kitrum-cloud.gmail.messages` where messageOwner = '{account_email}'"
            already_in_database_ids = [x['messageId'] for x in bigquery_handler.get_from_bigquery(query)]

            # GET ONLY MESSAGES WHICH ARE NOT IN OUR DATABASE
            gmail_handler.filter_messages(already_in_database_ids)

            # GET FULL INFORMATION FOR EACH RELEVANT MESSAGE
            print("\tGET NEW MESSAGES FROM GMAIL")
            gmail_handler.get_batch_message_details()

            # GET MESSAGE DETAILS PREPARED TO BE UPLOADED TO BIGQUERY
            print("\tPREPARE MESSAGES TO BE STORED IN BIGQUERY")
            gmail_handler.messages_to_bq_format()

            if gmail_handler.files_to_upload:
                print("\tUPLOADING FILES TO GOOGLE DRIVE")
                time.sleep(5)
                gdrive_handler.uploader(gmail_handler.files_to_upload)
            else:
                print("\tNO FILES TO UPLOAD TO GOOGLE DRIVE")

            # INSERT MESSAGES TO BIGQUERY
            print("\tINSERTING MESSAGES TO BIGQUERY")
            time.sleep(1)
            if len(gmail_handler.messages_for_bq) > 0:
                print(f"\t\t{len(gmail_handler.messages_for_bq)} Messages are ready to be inserted into BigQuery")
                bigquery_handler.insert_to_bigquery(
                    array_splitter(gmail_handler.messages_for_bq, 500),
                    "kitrum-cloud.gmail.messages"
                )
            else:
                print("\t\tNo new messages to insert")

            sync_stats = {
                "login_status": login_status,
                "new_messages": len(gmail_handler.status_tracking['success']),
                "downloaded_messages": len(gmail_handler.messages_for_bq),
                "errors": gmail_handler.status_tracking['failure']
            }
            results['sync_results'][account_email] = sync_stats
            gdrive_handler.delete_local_files()
        except Exception as e:
            results['sync_results'][account_email] = {"status": "Global Error"}
            print("Error Here")
            print(e)
            print("Error occured. Moving to the next account")

    print("\tINSERTING ATTACHMENTS TO BIGQUERY")
    time.sleep(2)
    if len(gdrive_handler.files_for_bq) > 0:
        print(f"\t\t{len(gdrive_handler.files_for_bq)} Attachments are ready to be inserted into BigQuery")
        bigquery_handler.insert_to_bigquery(
            array_splitter(gdrive_handler.files_for_bq, 1000),
            "kitrum-cloud.gmail.attachments"
        )
    else:
        print("\t\tNo new attachments to insert")
    results['downloaded_attachments'] = len(gdrive_handler.files_for_bq)
    results['sync_results'] = json.dumps(results['sync_results'])
    bigquery_handler.insert_to_bigquery(
        array_splitter([results], 1000),
        "kitrum-cloud.gmail.gmail_syncs"
    )
    return results


if __name__ == '__main__':
    print(f"---> Started at {datetime.now()}")
    sync_results = main()
    print(f"---> Completed at {datetime.now()}")
