from src.fetch_attachment import access_email_and_download_attachments_and_extract
from src.process_excel import process_excel_and_insert_to_database

if __name__ == '__main__':
    access_email_and_download_attachments_and_extract()
    process_excel_and_insert_to_database()