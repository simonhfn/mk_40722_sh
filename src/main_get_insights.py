from src.analysis import preprocess, perform_analysis_on_events, pretty_printing
from src.data_loader import get_local_file, BUCKET, get_file, create_file_name
from src.database import SQLDatabase, get_max_user_activity, get_max_country_activity, get_max_companies_activity

if __name__ == "__main__":
    ## Run analysis on a local and unique file, useful to explore the data.
    # df = get_local_file()
    # df = preprocess(df)
    #
    # res = perform_analysis_on_events(df, {"timestamp", "email", "country", "company"})
    # for colOI in res.keys():
    #     pretty_printing(res[colOI], colOI)

    ### Run analysis on db, which means all events.csv that have been ingested.
    max_activity_user = get_max_user_activity()
    print(max_activity_user.to_markdown())

    max_activity_country = get_max_country_activity()
    print(max_activity_country.to_markdown())

    max_activity_companies = get_max_companies_activity()
    print(max_activity_companies.to_markdown())
