import os
import logging
from datetime import datetime
import pandas as pd
import json
from datamule import Portfolio
import shutil
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("sec_downloader")

def get_user_input():
    print("==== SEC Filing Downloader ====")
    print("This script downloads SEC filings for specified companies.")
    
    ticker_input = input("Enter ticker symbols separated by commas: ")
    tickers = [ticker.strip().upper() for ticker in ticker_input.split(',')]
    
    print("Enter date range:")
    start_year = input("Start Year (YYYY): ")
    start_month = input("Start Month (MM): ")
    
    use_current_date = input("Use current date as end date? (y/n): ").lower() == 'y'
    if use_current_date:
        end_date = datetime.now()
        end_year = str(end_date.year)
        end_month = str(end_date.month)
        end_day = str(end_date.day)
    else:
        end_year = input("End Year (YYYY): ")
        end_month = input("End Month (MM): ")
        end_day = input("End Day (DD): ")
    
    start_date = f"{start_year}-{start_month.zfill(2)}-01"
    end_date = f"{end_year}-{end_month.zfill(2)}-{end_day.zfill(2)}"
    
    filing_types = []
    if input("Download 10-K filings? (y/n): ").lower() == 'y': filing_types.append('10-K')
    if input("Download 10-Q filings? (y/n): ").lower() == 'y': filing_types.append('10-Q')
    if input("Download 8-K filings? (y/n): ").lower() == 'y': filing_types.append('8-K')
    
    return tickers, start_date, end_date, filing_types

def main():
    logger.info("Starting SEC filings download")
    tickers, start_date, end_date, filing_types = get_user_input()
    
    if not tickers or not filing_types:
        logger.error("Missing ticker symbols or filing types. Exiting.")
        return
    
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    
    processed_count = 0
    
    for ticker in tickers:
        ticker_dir = os.path.join(output_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        logger.info(f"Processing {ticker}...")
        
        for filing_type in filing_types:
            filing_dir = os.path.join(ticker_dir, filing_type)
            os.makedirs(filing_dir, exist_ok=True)
            
            logger.info(f"Downloading {filing_type} filings for {ticker}...")
            portfolio = Portfolio(filing_dir)
            
            try:
                portfolio.download_submissions(
                    ticker=ticker,
                    submission_type=[filing_type],
                    filing_date=(start_date, end_date)
                )
                
                # Count actual submissions with content
                submission_count = 0
                for submission in portfolio.submissions:
                    try:
                        # Create a unique directory for each submission
                        submission_id = datetime.now().strftime("%Y%m%d%H%M%S") + str(submission_count)
                        submission_dir = os.path.join(filing_dir, submission_id)
                        os.makedirs(submission_dir, exist_ok=True)
                        
                        # Save metadata if available
                        if hasattr(submission, 'metadata') and submission.metadata:
                            with open(os.path.join(submission_dir, 'metadata.json'), 'w') as f:
                                json.dump(submission.metadata, f, indent=2)
                        
                        # Try to process documents
                        has_documents = False
                        try:
                            for doc in submission.document_type(filing_type):
                                has_documents = True
                                try:
                                    # Parse and save document data
                                    parsed_data = doc.parse()
                                    with open(os.path.join(submission_dir, 'parsed_data.json'), 'w') as f:
                                        json.dump(parsed_data, f, indent=2)
                                except Exception as e:
                                    logger.error(f"Error parsing document: {str(e)}")
                        except (KeyError, AttributeError) as e:
                            logger.warning(f"Could not access documents: {str(e)}")
                        
                        if has_documents:
                            submission_count += 1
                        else:
                            # Remove empty submission directory
                            shutil.rmtree(submission_dir)
                    except Exception as e:
                        logger.error(f"Error processing submission: {str(e)}")
                
                processed_count += submission_count
                print(f"Found {submission_count} {filing_type} filings for {ticker}")
                
            except Exception as e:
                logger.error(f"Error downloading {filing_type} for {ticker}: {str(e)}")
    
    print(f"\nTotal filings downloaded and processed: {processed_count}")
    print("Files are saved in the download_data/ directory")

if __name__ == "__main__":
    main()