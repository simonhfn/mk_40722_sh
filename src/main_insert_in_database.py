from src.analysis import preprocess, perform_analysis_on_events, pretty_printing
from src.data_loader import get_local_file, BUCKET, get_file, create_file_name
from src.database import SQLDatabase, check_if_data_in_db
from src.database import add_email, add_timestamp, add_countries, add_companies, add_record_ingestion


# we can think of deployment the following code in a lambda, that's triggered on the 1st of every month
# and collect last events.csv file. We can trigger it on a scheduled using eventBridge in AWS and the following
# chron expression: cron(0 10 1 * * ? *) i.e. will run every 1st day of the month
# yesterday = datetime.datetime.now() - datetime.timedelta(day=1)
# YEAR = yesterday.year
# MONTH = yesterday.month

YEAR = "2021"
MONTH = "04"

if __name__ == "__main__":
    db = SQLDatabase()

    df = get_file(BUCKET, create_file_name(YEAR, MONTH))
    # df = get_local_file()

    print("File got from s3, being processed ...........")
    df = preprocess(df)
    print(df.size)

    print(f"getting insights from year: {YEAR} and month: {MONTH}")
    res = perform_analysis_on_events(df, {"timestamp", "email", "country", "company"})

    if not check_if_data_in_db(YEAR, MONTH):
        print("ingesting in database........")
        add_companies(res['company'].to_dict("records"), YEAR, MONTH)
        add_email(res['email'].to_dict("records"), YEAR, MONTH)
        add_timestamp(res['timestamp'].to_dict("records"), YEAR, MONTH)
        add_countries(res['country'].to_dict("records"), YEAR, MONTH)
        add_record_ingestion(YEAR, MONTH)

        db.commit()
        print("done.")
