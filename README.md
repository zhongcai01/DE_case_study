# Part 1. Python Task

This project is done in python 3.11

The code for accessing email, download attachments and extract the zipfiles are found in the [fetch_attachments.py](src.fetch_attachment.py)


The outlook email used to test is a personal email which requires a delegate permission to access the emails in the account. 

This code is assuming that the email only contains the "DE Case Study" zip file. The code assumes the files inside the zipfile are named "Raw Active", "Raw Customer", "Raw Orders", "Raw Service".

Constants that are used for authenticate for outlook are stored in constants.json

output of the extracted files are stored in the 'extracted_folder_path' value of json 


The code for processing the excel files are in the [process_excel.py](src.process_excel.py). The excel files are processed with pandas package and connects to a postgresql database using psycopg2 package.


# Part 2. SQL Task
The [flattened_table_query.sql](flattened_table_query.sql) contains the query for the flattened_table. It joins the Order Table and Active Table. This joined table should be able to track:
- Whether a service is a new signup.
- Whether a service has made a churn event.
- Whether the new signup is a transfer
- The current status of the service (e.g., active or inactive)

The daily, weekly, monthly frequency can be aggregated on the REPORT_DATE using the DATE_TRUNC function. 

