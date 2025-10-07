# Maryland Public Service Commission Case Scraper

This project is a proof of concept for a scraper that extracts files related to recent cases and rulemaking cases on the Maryland Public Service Commission website.

It makes use of BeautifulSoup 4 for HTML parsing.  This code was tested on Windows using Python 3.12, though it likely works on Unix and other Python versions.

## Running the Scraper

1. Install the requirements with `pip install -r requirements.txt`. It is recommended to use a virtual environment.
2. Run `python crawler.py` - on some systems, `python3` may be the correct command.
3. This code creates an `output/` directory, containing `data_mart.csv` and folders with the downloaded files - one folder per case.  `data_mart.csv` contains one row per downloaded file, with metadata on the case it is for, the document in question, and the downloaded file path.  Cases with no files will not be included.
4. In case of any issues, logs are saved to `md_case_scrape.log` in the project's root directory.

Note that rerunning the script will overwrite `data_mart.csv` but will not delete previously-downloaded files.

## Expanding the Scraper

### Pipeline Design

To expand this into a production-grade pipeline, the scraper needs to be more robust. The scraper relies heavily on the standardized format of HTML tags in the Commission website, and any change would break it. Additionally, the pipeline should flag any such errors. A production pipeline needs to proactively flag the errors instead of simply logging them.  The code for identifying the latest rulemaking case would benefit from a max duration in the pipeline, and likely a rewrite of the code itself, in case of such a change.

A production pipeline would need to determine which cases to download on each run. It would be best to identify which have new uploads and process only these. The pipeline could then merge new rows into the data mart, rather than completely rebuilding it each time (reducing time and cost).

Depending on the use case, files could be downloaded asynchronously, as this is the slowest part of the process and is not required to obtain the metadata. As these files are quite large, cloud storage likely makes more sense for the files as well.

A production pipeline would likely put cases and files into separate tables as the first step, as well. Cases with no files exist but are excluded by the current scraper output.  A later stage of the pipeline could join these tables to produce the desired data mart.

The Commission website sometimes allocates the same description to groups of loosely related files. For instance, case 9818 (https://webpscxb.psc.state.md.us/DMS/case/9818) item 1 contains several different types of files under one date and description. A production pipeline that plans to use these files might attempt to produce their own file summary in addition to the website metadata, at the end of the process.

### Proposed Technology Stack

This pipeline could be deployed on AWS, orchestrated with a tool like Dagster or Airflow, with dbt used to transform data into the final data mart.

AWS ECS allows for the execution of the scraper with computing resources scaling based on need, while standardizing the runtime environment.  S3 would be a good choice for file storage, since it is inexpensive and feeds in well to the rest of the infrastructure.  Since this data is qualitative, a row-based solution like Aurora Postgres is a low maintenance, scalable database solution for the data mart.  Further, it reduces the maintenance needed on the user side.

Dagster allows for orchestrating each step together and provides good observability into the pipeline and errors that may occur. Together with dbt, it is able to prevent pushing bad data downstream should changes to the Commission website break the pipeline and can flag to users that an error has occurred. Dagster can work with AWS ECS to spin up resources for the processing; it could spin up additional resources to download files concurrently. Dagster can also assist with determining which cases to reprocess, potentially partitioning based on a last updated date.

Dagster and dbt combine well to pull case and file data into separate tables before joining, as Dagster can orchestrate the separate assets and manage dependencies and make them available for other uses.  dbt is able to validate the data and its transformations with tests, and the process to combine the data into the final data mart is suitable for a straightforward SQL solution. An incremental materialization would allow for a solution that does not process every case each run. dbt connects seamlessly to AWS Aurora Postgres, as well.
