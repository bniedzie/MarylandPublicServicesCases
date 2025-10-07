"""
This is a web crawler for the Maryland Public Services Commission website.
It downloads files from recent cases and rulemaking cases and creates a csv with metadata.

Author: Benjamin Niedzielski
"""

import os
import csv
import requests
import logging
from bs4 import BeautifulSoup, Tag


# In a production pipeline, these would be better in a customizable config.
OUTPUT_DIR = "output"
CSV_OUTPUT_PATH = "data_mart.csv"
BASE_URL = "https://webpscxb.psc.state.md.us"
LOG_PATH = "md_case_scrape.log"
CASES_TO_PROCESS = 5


# These functions might be shared across scrapers and be better placed in a package.
def get_logger(level=logging.DEBUG) -> logging.Logger:
    """
    Creates a logger to use, outputting to a file at LOG_PATH.
    :param level: The log level to use. Default DEBUG.
    :return: The logger to use with this scraper.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def create_directory(directory_path: str, logger: logging.Logger) -> bool:
    """
    Creates a new directory in the file system at directory_path.
    :param directory_path: The path of the directory to create.
    :param logger: The logger to use.
    :return: False if an error, or True otherwise. Returns True if the directory already exists.
    """
    try:
        os.mkdir(directory_path)
        logger.debug(f"Directory '{directory_path}' created.")
    except FileExistsError:
        logger.debug(f"Directory '{directory_path}' already exists.")
    except Exception as e:
        logger.error(f"Error creating directory for output: {e}")
        return False
    return True


def download_file(file_url: str, file_save_path: str, logger: logging.Logger) -> bool:
    """
    Given a URL representing a file, saves it to a specified path.
    :param file_url: The URL of the file to download.
    :param file_save_path: The path to save the file to.
    :param logger: The logger to write to.
    :return: True if successful, or False otherwise.
    """
    try:
        r = requests.get(file_url, stream=True)
        r.raise_for_status()
        with open(file_save_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks.
                    f.write(chunk)
    except Exception as e:
        logger.error(f"Failed to read webpage with error {e}")
        return False
    return True


def write_csv(data: list[list], output_path: str, logger: logging.Logger) -> bool:
    """
    Writes data to a csv file at a specified path.
    :param data: The data to write, in the format o list of rows, where each row is a list of cells.
    :param output_path: The file path to write to.
    :param logger: The logger to use.
    :return: True if successful, or False otherwise.
    """
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)
    except Exception as e:
        logger.error(f"Error writing csv output: {e}")
        return False
    return True


# MD Scraper specific functions.
def process_case_data(url: str, case_num: int | str, logger: logging.Logger) -> list:
    """
    Given a url containing MD case data, downloads all relevant files and returns a list with metadata on them and
    the case.
    NOTE: Does not return cases with no files (i.e., result is an INNER JOIN rather than a LEFT JOIN).
    :param url: A url containing case data, e.g., https://webpscxb.psc.state.md.us/DMS/rm/rm91.
    :param case_num: The ID of the case. A number for normal cases, a string starting with sm for rulemaking ones.
    :param logger: The logger to write to.
    :return: A list of files, where each file is a list with case and file metadata:
        [case number, case description, case date, document description, document name, document date, download path]
    """
    logger.info(f"Processing case number {case_num} from {url}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        case_page = r.text
    except Exception as e:
        logger.error(f"Failed to read webpage with error {e}")
        return []

    if case_page != "":
        case_data = []
        try:
            soup = BeautifulSoup(case_page, "html.parser")
            # Both cases and rulemaking cases have the same html format.
            # This scraper relies on the existing structure.
            case_date = soup.find(id="ContentPlaceHolder1_hFiledDate").string.strip()
            # This value starts with "Date Filed : "
            if ":" in case_date:
                case_date = case_date.split(":")[1].strip()
            case_description = soup.find(id="ContentPlaceHolder1_hCaseCaption").string.strip()
            files_table = soup.find(id="caserulepublicdata")
            case_file_data = process_case_file_data(files_table, case_num, logger)
            for case_file_row in case_file_data:
                case_data.append(
                    [
                        case_num,
                        case_description,
                        case_date,
                        case_file_row[0],  # Document Description
                        case_file_row[1],  # Document filename
                        case_file_row[2],  # Document date
                        case_file_row[3]   # File location of downloaded document
                    ]
                )
            logger.info(f"Finished processing case number {case_num}")
        except Exception as e:
            logger.error(f"Failed to process webpage with error {e}")
            return []
        return case_data
    return []


def process_case_file_data(files_table: Tag, case_no: int | str, logger: logging.Logger) -> list:
    """
    Given a Tag representing a table of public file uploads for a case, download the files.
    Returns metadata about the downloaded files.
    NOTE: Returns only those rows with files (i.e., INNER JOIN rather than LEFT JOIN)
    :param files_table: A BeautifulSoup Tag representing the table of file uploads for the case.
    :param case_no: The ID of the case. A number for normal cases, a string starting with sm for rulemaking ones.
    :param logger: The logger to write to.
    :return: A list of downloaded files, where each file is a list with file metadata:
        [document description, document name, document date, download path]
    """
    file_row = 0
    case_data = []
    for fileInfo in files_table.tbody.find_all("tr"):
        file_row = file_row + 1

        # The 3 columns are number, subject, and date.  Number may contain a link to file, in a span's data-pdf attr.
        file_info_cols = fileInfo.find_all("td", limit=3)
        if len(file_info_cols) != 3:
            logger.error(f"Document data table is incorrectly formatted for row {file_row}.")
            continue
        file_download_path = file_info_cols[0].span["data-pdf"]
        file_download_path = f"{BASE_URL}{file_download_path}"
        # Use the raw description akin to a bronze table, though it likely makes sense to clean this for production use.
        file_description = file_info_cols[1].get_text().strip()
        file_date = file_info_cols[2].string.strip()

        # Use the row number to create sub-folders for this case.
        # While the number itself is arbitrary, this should help handle files with the same name uploaded on
        # different dates.
        download_dir = f"{OUTPUT_DIR}/{case_no}/{file_row}"

        logger.info(f"Retrieving files for row {file_row}")
        downloaded_files = download_files(file_download_path, download_dir, logger)
        for downloaded_file in downloaded_files:
            case_data.append(
                [
                    file_description,
                    downloaded_file[0],  # Document Name
                    file_date,
                    downloaded_file[1]   # Download Path
                ]
            )
    return case_data


def download_files(file_listing_url: str, download_dir: str, logger: logging.Logger) -> list:
    """
    Given the URL with a list of files in <span>s with a data-pdf attribute, download all files
    and return a list of the files, where each file is represented as a list containing the file name and download path.
    :param file_listing_url: The URL to scrape for file download information.
    :param download_dir: The directory to download files to.
    :param logger: The logger to write to.
    :return: A list of downloaded files, where each file is a list with file metadata:
        [document name, download path]
    """
    try:
        r = requests.get(file_listing_url)
        r.raise_for_status()
        file_page = r.text
    except Exception as e:
        logger.error(f"Failed to read webpage with error {e}")
        return []

    if file_page != "":
        downloaded_files = []
        try:
            soup = BeautifulSoup(file_page, "html.parser")
            # Create a folder only if files are found.
            if soup.find(attrs={"data-pdf": True}):
                if not create_directory(download_dir, logger):
                    return []

            for fileDownload in soup.find_all("span", attrs={"data-pdf": True}):
                pdf_path = f"{BASE_URL}{fileDownload['data-pdf']}"
                download_path = f"{download_dir}/{fileDownload.get_text().strip()}"
                if download_file(pdf_path, download_path, logger):
                    logger.info(f"Successfully downloaded file {download_path}")
                    downloaded_files.append(
                        [
                            fileDownload.get_text().strip(),  # File name
                            download_path                     # Downloaded file path
                        ]
                    )
                else:
                    logger.error(f"Failed to download file {download_path}")
        except Exception as e:
            logger.error(f"Failed to process webpage with error {e}")
        return downloaded_files
    return []


def get_latest_case(logger: logging.Logger) -> int:
    """
    Returns the ID of the most recent case.
    :param logger: The logger to write to.
    :return: The ID of the most recent case. Returns -1 if unable to access the website.
    """
    try:
        r = requests.get(f"{BASE_URL}/DMS/recentcases")
        r.raise_for_status()
        latest_cases_page = r.text
    except Exception as e:
        logger.error(f"Failed to load recent cases page with error {e}")
        return -1

    if latest_cases_page != "":
        try:
            soup = BeautifulSoup(latest_cases_page, "html.parser")
            latest_case = soup.find(id="ContentPlaceHolder1_RptRecentCasesList_lnkbtnCaseNum_0")
            if latest_case is None:
                logger.error("Unable to identify the most recent case.")
                return -1
            return int(latest_case.get_text().strip())
        except Exception as e:
            logger.error(f"Failed to process recent cases webpage with error {e}")
            return -1
    return -1


def get_latest_rulemaking_case(logger: logging.Logger) -> int:
    """
    Returns the ID of the most recent rulemaking case.
    :param logger: The logger to write to.
    :return: The ID of the most recent case.
    """
    # Unlike normal cases, there seems to be no page listing recent rulemaking cases.
    # Use the latest known ID as a starting point to find the true latest.
    latest_id = 91
    # The pipeline should likely have a timeout, or this should have a limit.
    # A change to the "Data not found" format would cause problems here.
    while True:
        try:
            r = requests.get(f"{BASE_URL}/DMS/rm/rm{latest_id + 1}")
            r.raise_for_status()
            latest_cases_page = r.text
        except Exception as e:
            logger.error(f"Failed to load rulemaking page for rm{latest_id + 1} with error {e}")
            return latest_id

        if latest_cases_page != "":
            try:
                soup = BeautifulSoup(latest_cases_page, "html.parser")
                not_found_tag = soup.find(id="ContentPlaceHolder1_divCaseRulePublicNotFound")
                if not_found_tag is not None:
                    return latest_id
                else:
                    latest_id = latest_id + 1
            except Exception as e:
                logger.error(f"Failed to process rulemaking page cases webpage with error {e}")
                return latest_id
        else:
            return latest_id


def main():
    """
    Downloads case files from the Maryland Public Service Commission website and produces a csv with metadata.
    """
    logger: logging.Logger = get_logger()
    if not create_directory(OUTPUT_DIR, logger):
        return

    case_file_data = [
        [
            "Case Number",
            "Case Description",
            "Case Date",
            "Document Description",
            "Document Filename",
            "Document Date",
            "File Location of Downloaded Document"
        ]
    ]

    # Download recent cases.
    case_id = get_latest_case(logger)
    if case_id == -1:
        logger.error("Could not identify the latest case")
    else:
        logger.debug(f"Identified latest case ID as {case_id}")
        for i in range(CASES_TO_PROCESS):
            url = f"{BASE_URL}/DMS/case/{case_id}"
            if not create_directory(f"{OUTPUT_DIR}/{case_id}", logger):
                return
            case_file_data = case_file_data + process_case_data(url, case_id, logger)
            case_id = case_id - 1

    # Download recent rulemaking cases.
    rulemaking_case_id = get_latest_rulemaking_case(logger)
    logger.debug(f"Identified latest rulemaking case ID as {rulemaking_case_id}")
    for i in range(CASES_TO_PROCESS):
        url = f"{BASE_URL}/DMS/rm/rm{rulemaking_case_id}"
        if not create_directory(f"{OUTPUT_DIR}/rm{rulemaking_case_id}", logger):
            return
        case_file_data = case_file_data + process_case_data(url, f"rm{rulemaking_case_id}", logger)
        rulemaking_case_id = rulemaking_case_id - 1

    write_csv(case_file_data, f"{OUTPUT_DIR}/{CSV_OUTPUT_PATH}", logger)


if __name__ == "__main__":
    main()
