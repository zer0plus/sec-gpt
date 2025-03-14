import os
import time
from datetime import datetime
import calendar
import pandas as pd
from tqdm import tqdm
import logging
import re
import sys

from sec_edgar_downloader import Downloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sec_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sec_downloader")

def get_user_input():
    """Get user input for tickers, date range, and filing types"""
    # Get tickers from user
    ticker_input = input("Enter ticker symbols separated by commas (e.g., TSLA,GM,F,PLUG): ").strip()
    
    if not ticker_input:
        logger.error("No tickers provided. Exiting...")
        sys.exit(1)
    
    tickers = [ticker.strip().upper() for ticker in ticker_input.split(',')]
    
    # Get date range from user
    print("\nEnter date range for SEC filings:")
    start_year = input("Start Year (YYYY): ").strip()
    start_month = input("Start Month (MM): ").strip()
    
    # Validate start date inputs
    try:
        start_date = f"{start_year}-{start_month.zfill(2)}-01"
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid start date format. Using default of 3 years ago.")
        three_years_ago = datetime.now().year - 3
        start_date = f"{three_years_ago}-01-01"
    
    # For end date, offer current date as default
    use_current_date = input("Use current date as end date? (y/n): ").strip().lower()
    
    if use_current_date == 'y' or use_current_date == 'yes':
        end_date = datetime.now().strftime("%Y-%m-%d")
    else:
        end_year = input("End Year (YYYY): ").strip()
        end_month = input("End Month (MM): ").strip()
        
        # Validate end date inputs
        try:
            # Get last day of the month
            last_day = str(calendar.monthrange(int(end_year), int(end_month))[1])
            end_date = f"{end_year}-{end_month.zfill(2)}-{last_day}"
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid end date format. Using current date.")
            end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Get filing types from user
    filing_types = []
    
    get_10k = input("Download 10-K filings? (y/n): ").strip().lower()
    if get_10k == 'y' or get_10k == 'yes':
        filing_types.append("10-K")
    
    get_10q = input("Download 10-Q filings? (y/n): ").strip().lower()
    if get_10q == 'y' or get_10q == 'yes':
        filing_types.append("10-Q")
    
    if not filing_types:
        logger.error("No filing types selected. Defaulting to 10-K and 10-Q.")
        filing_types = ["10-K", "10-Q"]
    
    # Summarize inputs for the user
    print("\nDownload Summary:")
    print(f"Tickers: {', '.join(tickers)}")
    print(f"Date Range: {start_date} to {end_date}")
    print(f"Filing Types: {', '.join(filing_types)}")
    print()
    
    return tickers, start_date, end_date, filing_types

def create_directories():
    """Create directories for downloads"""
    downloads_dir = "sec_filings"
    os.makedirs(downloads_dir, exist_ok=True)
    
    logger.info("Created all necessary directories")
    return downloads_dir

def download_sec_filings(downloads_dir, tickers, start_date, end_date, filing_types):
    """Download filings using sec-edgar-downloader library"""
    logger.info("Starting downloads with sec-edgar-downloader")
    
    # Prompt for email (required by SEC)
    email = input("Enter your email address for SEC EDGAR access: ").strip()
    if not email or '@' not in email:
        logger.warning("Invalid email format. Using placeholder email.")
        email = "user@example.com"
    
    company_name = input("Enter your company/organization name (or your name): ").strip()
    if not company_name:
        company_name = "Individual Investor"
    
    # Create downloader instance
    dl = Downloader(company_name, email, downloads_dir)
    
    # Track progress
    results = {
        "ticker": [], 
        "filing_type": [], 
        "filing_date": [], 
        "accession_number": [], 
        "file_count": [],
        "period_of_report": []
    }
    
    # Download for each ticker and filing type
    for ticker in tqdm(tickers, desc="Processing tickers"):
        for filing_type in filing_types:
            try:
                logger.info(f"Downloading {filing_type} filings for {ticker}")
                
                # Determine appropriate limit based on filing type and date range
                start_year = int(start_date.split('-')[0])
                end_year = int(end_date.split('-')[0])
                years_difference = end_year - start_year + 1
                
                # For 10-K: typically 1 per year, for 10-Q: typically 3 per year
                if filing_type == "10-K":
                    limit = max(5, years_difference)  # At least 5 for good measure
                else:  # 10-Q
                    limit = max(15, years_difference * 4)  # At least 15 for good measure
                
                dl.get(
                    filing_type, 
                    ticker, 
                    limit=limit,
                    after=start_date,
                    before=end_date
                )
                
                # Now correctly look in the downloads_dir
                ticker_path = f"{downloads_dir}/sec-edgar-filings/{ticker}/{filing_type}"
                if os.path.exists(ticker_path):
                    for root, dirs, files in os.walk(ticker_path):
                        if len(files) > 0 and root != ticker_path:
                            accession_number = os.path.basename(root)
                            filing_date = "Unknown"
                            period_of_report = "Unknown"
                            
                            for file in files:
                                if file.endswith(".txt") and "filing-details" in file:
                                    try:
                                        with open(os.path.join(root, file), 'r', encoding='utf-8', errors='replace') as f:
                                            content = f.read(10000)
                                            
                                            # Extract filing date
                                            date_match = re.search(r'FILED AS OF DATE:\s+(\d{8})', content)
                                            if date_match:
                                                date_str = date_match.group(1)
                                                filing_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                                            
                                            # Extract period of report
                                            period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s+(\d{8})', content)
                                            if period_match:
                                                period_str = period_match.group(1)
                                                period_of_report = f"{period_str[:4]}-{period_str[4:6]}-{period_str[6:8]}"
                                    except Exception as e:
                                        logger.warning(f"Error extracting date from file {file}: {str(e)}")
                            
                            results["ticker"].append(ticker)
                            results["filing_type"].append(filing_type)
                            results["filing_date"].append(filing_date)
                            results["accession_number"].append(accession_number)
                            results["file_count"].append(len(files))
                            results["period_of_report"].append(period_of_report)
                
                # Be nice to SEC servers
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error downloading {filing_type} for {ticker}: {str(e)}")
                time.sleep(10)
    
    # Save results to CSV
    results_df = pd.DataFrame(results)
    results_df.to_csv("sec_filings_inventory.csv", index=False)
    logger.info(f"Downloads complete. Results saved to sec_filings_inventory.csv")
    
    return results_df

def analyze_results(results_df):
    """Analyze download results and create summary statistics"""
    logger.info("Analyzing download results")
    
    if results_df.empty:
        logger.warning("No results to analyze")
        return pd.DataFrame(columns=["ticker", "filing_type", "filing_count", "total_files"])
    
    # Create summary by ticker and filing type
    summary = results_df.groupby(['ticker', 'filing_type']).agg({
        'accession_number': 'count',
        'file_count': 'sum'
    }).reset_index()
    
    # Rename columns for clarity
    summary.rename(columns={
        'accession_number': 'filing_count',
        'file_count': 'total_files'
    }, inplace=True)
    
    # Save summary
    summary.to_csv("filing_summary.csv", index=False)
    logger.info("Analysis complete. Summary saved to filing_summary.csv")
    
    return summary

def main():
    """Main function to execute the download process"""
    logger.info("Starting SEC filings download")
    
    print("==== SEC Filing Downloader ====")
    print("This script will download SEC filings for specified companies.")
    print("You'll be asked to enter ticker symbols, date range, and filing types.")
    print("=================================\n")
    
    # Get user input
    tickers, start_date, end_date, filing_types = get_user_input()
    
    # Create directories
    downloads_dir = create_directories()
    
    # Download filings
    results_df = download_sec_filings(downloads_dir, tickers, start_date, end_date, filing_types)
    
    # Analyze results
    summary = analyze_results(results_df)
    
    # Log summary
    logger.info("Download process complete!")
    
    if not results_df.empty:
        logger.info(f"Total filings downloaded: {len(results_df)}")
        logger.info(f"Total files downloaded: {results_df['file_count'].sum()}")
    
    print("\nDownload Summary:")
    print(summary)
    
    print("\nDownloaded files are organized as follows:")
    print(f"{downloads_dir}/sec-edgar-filings/<ticker>/<form_type>/<accession_number>/")
    print("\nData is ready for further analysis!")

if __name__ == "__main__":
    main()