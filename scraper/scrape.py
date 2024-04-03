import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import time
import os

def scrape(team, season):

    urls = get_urls(team, season)
    counter = 0

    for url in urls:

        print(f"Iteration {counter}")
        counter = counter + 1

        # Extract the filename form the URL
        filename = url.split('/')[-1].replace('.html', '.csv')

        # Build the full path to the file
        file_path = os.path.join('data', 'raw', season, team, filename)

        # Skips if file exists
        if os.path.isfile(file_path):
            continue

        # Add 4s delay to prevent website block
        time.sleep(4)

        # Run the functions
        scraped = scrape_raw_pbp(url, filename, team, season)

        if scraped:
            clean_pbp(filename, team, season)
        else:
            print(f"Failed to scrape {filename}")

    combine(team, season)

def get_urls(team, season):

    source = f"https://www.basketball-reference.com/teams/{team}/{season}_games.html"

    urls = []

    # Send a request to the URL
    response = requests.get(source)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all links within the table#games that start with /boxscore/ in the href
        links = soup.select('table#games a[href^="/boxscores/"][href$=".html"]')

        # Filtering links that contain any textual content (future games)
        links = [link for link in links if link.text.strip()]

        # Process each link individually
        for link in links:
            # Get the href attribute of the link
            href = link['href']
            # Extract the last part of the href (the filename)
            filename = href.split('/')[-1].replace('.html', '')

            url = f"https://www.basketball-reference.com/boxscores/pbp/{filename}.html"

            # Add the processed filename to the urls list
            urls.append(url)

    return urls

def scrape_raw_pbp(url, filename, team, season):

    raw_directory = f"data/raw/{season}/{team}/"

    # Send a request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table with id 'pbp'
        table = soup.find('table', {'id': 'pbp'})

        # Check if the table was found
        if table:

            rows = [
                ['A', 'B','C','D','E','F','Event']
            ]

            try:
                # Process the table headers
                headers = [th.getText() for th in table.find_all('tr')[0].find_all(['th', 'td'])]
            except Exception as e:
                # Catch any other exception that wasn't caught by the specific except blocks
                print(f"Error on table for game {filename}: {e}")
                return False

            # Initialize a list to hold all rows of the table
            rows.append(headers)

            # Process the table rows
            for tr in table.find_all('tr')[1:]:
                # Handle rows with 'rowspan'
                for cell in tr.find_all(['td', 'th'], rowspan=True):
                    # Get the rowspan value and replicate the cell content across the spanned rows
                    rowspan_val = int(cell['rowspan'])
                    for i in range(1, rowspan_val):
                        tr.find_next_siblings('tr')[i - 1].insert(len(tr.find_all(['td', 'th'])), cell)

                # Extract text from each cell, including both 'td' and 'th' elements
                row = [cell.getText() for cell in tr.find_all(['td', 'th'])]

                # Check for specific class in each cell and append relevant text
                event = ""
                for cell in tr.find_all(['td', 'th']):
                    if 'bbr-play-leadchange' in cell.get('class', []):
                        event = "Lead change"
                        break
                    elif 'bbr-play-tie' in cell.get('class', []):
                        event = "Tie"
                        break

                row.append(event)

                if row:
                    rows.append(row)

            # Check if the directory exists, and if not, create it
            if not os.path.exists(raw_directory):
                os.makedirs(raw_directory)

            # Save the data to a CSV file
            with open(raw_directory + filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            print(f"The data has been successfully scraped and saved as '{filename}'.")
            return True
        else:
            print("Table with ID 'pbp' not found on the page.")
            return False
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        return False

def clean_pbp(filename, team, season):

    raw_directory = f"data/raw/{season}/{team}/"
    clean_directory = f"data/clean/{season}/{team}/"

    # Read the rawCSV file into a DataFrame
    df = pd.read_csv(raw_directory + filename)

    # Filter out rows containing specific texts in column "A"
    filter_texts = ['1st Q', '2nd Q', '3rd Q', '4th Q', '1st OT', '2nd OT', '3rd OT', '4th OT']
    df = df[~df['A'].astype(str).str.contains('|'.join(filter_texts))]

    # Remove duplicate rows, keeping the first occurrence
    df = df.drop_duplicates()

    # Drop columns 'C' and 'E'
    df.drop(['C', 'E'], axis=1, inplace=True)

    # Initialize the 'Quarter' column with 1
    df['Quarter'] = 1

    # Function to extract number from strings like "1st", "2nd", etc.
    def extract_number(text):
        match = re.search(r'\d+', text)
        return int(match.group()) if match else None

    # Iterate through the DataFrame rows
    current_quarter = 1
    for index, row in df.iterrows():
        if "Start of" in row['B']:
            quarter_num = extract_number(row['B'])
            if quarter_num:
                if "quarter" in row['B']:
                    current_quarter = quarter_num
                elif "overtime" in row['B']:
                    current_quarter = quarter_num + 4
        df.at[index, 'Quarter'] = current_quarter

    # Extract values from the first row to use as new column names
    new_column_names = df.iloc[0][['A', 'B', 'D', 'F']]

    # Rename the columns
    df.rename(
        columns={
            'A': new_column_names['A'],
            'B': new_column_names['B'],
            'D': new_column_names['D'],
            'F': new_column_names['F']
        }, inplace=True)

    # Drop the first row using iloc
    df = df.iloc[1:].reset_index(drop=True)

    # Iterate through the DataFrame rows and apply conditions
    previous_score = "0-0"  # Initialize the previous score
    for index, row in df.iterrows():
        second_column_value = row.iloc[1]  # Accessing the second column by its position

        if row['Time'] == "12:00.0" and row['Quarter'] == 1:
            df.at[index, 'Score'] = "0-0"
        elif "End of" in second_column_value or "Start of" in second_column_value:
            df.at[index, 'Score'] = previous_score
        elif pd.isna(df.at[index, 'Score']):
            df.at[index, 'Score'] = previous_score
        else:
            previous_score = df.at[index, 'Score']  # Update the previous score

    # Initialize columns 'MiamiScore', 'OpponentScore', and 'PointDifference'
    df['MiamiScore'] = 0
    df['OpponentScore'] = 0
    df['PointDifference'] = 0

    # Find the column index for "Miami" in the DataFrame
    miami_index = df.columns.get_loc("Miami")

    # Determine the indices for Miami and the opponent based on the location of "Miami"
    if miami_index == 1:
        miami = 0
        opponent = 1
    elif miami_index == 3:
        miami = 1
        opponent = 0

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Split the 'Score' column value into two parts using '-' as the separator
        scores = row['Score'].split('-')
        # Convert the split values to integers immediately
        miami_score = int(scores[miami])
        opponent_score = int(scores[opponent])

        # Assign the converted scores to their respective columns
        df.at[index, 'MiamiScore'] = miami_score
        df.at[index, 'OpponentScore'] = opponent_score
        # Calculate and assign the point difference
        df.at[index, 'PointDifference'] = miami_score - opponent_score

    # Constants
    quarter_seconds = 12 * 60
    ot_seconds = 5 * 60

    # Create "ElapsedTime" column and set to 0.0 initially
    df['ElapsedTime'] = 0.0

    # Iterate through each row
    for index, row in df.iterrows():
        # Split "Time" column value by ":"
        minutes, seconds = row['Time'].split(':')
        minutes = int(minutes)  # convert to int
        seconds = float(seconds)  # convert to float

        # Calculate remaining time in the quarter
        remaining_time_in_quarter = (minutes * 60) + seconds

        # Calculate elapsed time
        elapsed_quarters = 0
        if row['Quarter'] <= 4:
            elapsed_quarters = (row['Quarter'] - 1) * quarter_seconds
        else:
            ots = row['Quarter'] - 4
            elapsed_quarters = (4 * quarter_seconds) + ((ots - 1) * ot_seconds)

        elapsed_time = elapsed_quarters + (quarter_seconds - remaining_time_in_quarter)

        # Calculate total elapsed time
        df.at[index, 'ElapsedTime'] = elapsed_time

    # TEMP: Remove duplicated ElapsedTime rows
    # df = df.drop_duplicates(subset='ElapsedTime', keep='first')

    # Check if the directory exists, and if not, create it
    if not os.path.exists(clean_directory):
        os.makedirs(clean_directory)

    # Overwrite the original file with the filtered data
    df.to_csv(clean_directory + filename, index=False)

    # Display the message
    print(f"The file '{filename}' has been written with the filtered data.")

def combine(team, season):

    clean_directory = f"data/clean/{season}/{team}/"
    combined_directory = f"data/combined/"

    # Check if the directory exists, and if not, create it
    if not os.path.exists(combined_directory):
        os.makedirs(combined_directory)

    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(clean_directory) if f.endswith('.csv')]

    # Initialize an empty DataFrame to hold the combined data
    combined_df = pd.DataFrame()

    # Loop through each CSV file
    for file in csv_files:
        file_path = os.path.join(clean_directory, file)
        df = pd.read_csv(file_path)
        
        # Check the conditions and adjust the DataFrame accordingly
        if df.columns[1] == 'Miami':
            # Rename 2nd column to "Notes" and combine it with the 4th column
            df['Notes'] = df.iloc[:, 1].combine_first(df.iloc[:, 3])
            # Create new column "OpponentName" with the name of the 4th column
            df['OpponentName'] = df.columns[3]
            # Delete the 4th column
            df.drop(df.columns[[1, 3]], axis=1, inplace=True)

        elif df.columns[3] == 'Miami':
            # Rename 4th column to "Notes" and combine it with the 2nd column
            df['Notes'] = df.iloc[:, 3].combine_first(df.iloc[:, 1])
            # Create new column "OpponentName" with the name of the 2nd column
            df['OpponentName'] = df.columns[1]
            # Delete the 2nd column
            df.drop(df.columns[[1, 3]], axis=1, inplace=True)

        df['id'] = file.replace(".csv","")
        
        # Append the adjusted DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Sort quarter_combined_df by 'id' alphabetically and then by 'ElapsedTime' from lowest to highest
    combined_df = combined_df.sort_values(by=['id', 'ElapsedTime'])

    # Reset the index after sorting, if desired
    combined_df = combined_df.reset_index(drop=True)

    filename = f"{ season }-{ team }.csv"
    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(combined_directory + filename, index=False)

    print("Save combined file " + filename) 

    # Filter rows where Time is "12:00.0"
    time_12_df = combined_df[combined_df['Time'] == '12:00.0']

    # Filter the last occurrence of each id where Time is "0:00.0"
    time_0_df = combined_df[combined_df['Time'] == '0:00.0']
    last_time_0_df = time_0_df.groupby('id').tail(1)

    # Concatenate the filtered DataFrames
    quarter_combined_df = pd.concat([time_12_df, last_time_0_df]).drop_duplicates().reset_index(drop=True)

    # Sort quarter_combined_df by 'id' alphabetically and then by 'ElapsedTime' from lowest to highest
    quarter_combined_df = quarter_combined_df.sort_values(by=['id', 'ElapsedTime'])

    # Reset the index after sorting, if desired
    quarter_combined_df = quarter_combined_df.reset_index(drop=True)

    # Filter by Quarter scores only
    quarter_filename = f"{ season }-{ team }-quarters.csv"

    # Save the combined DataFrame to a new CSV file
    quarter_combined_df.to_csv(combined_directory + quarter_filename, index=False)

    print("Save combined file " + quarter_filename) 

    # Create minimal version for visualization

    # Step 1: Filter and group by 'id' and 'ElapsedTime', then get the first 'PointDifference' for each group
    filtered = quarter_combined_df[quarter_combined_df['ElapsedTime'].isin([720.0, 1440.0, 2160.0, 2880.0])]
    grouped = filtered.groupby(['id', 'ElapsedTime'])['PointDifference'].first().reset_index()

    # Step 2: Pivot the table to have 'ElapsedTime' as columns
    pivot_table = grouped.pivot(index='id', columns='ElapsedTime', values='PointDifference')

    # Renaming columns to match Q1, Q2, Q3, Q4
    pivot_table.columns = ['Q1', 'Q2', 'Q3', 'Q4']

    # Step 3: Merge with the distinct 'id' and 'OpponentName' from the original DataFrame
    opponents = quarter_combined_df[['id', 'OpponentName']].drop_duplicates()
    quarter_viz_df = pd.merge(opponents, pivot_table, on='id')

    # Filter by Quarter scores only
    quarter_viz_filename = f"{ season }-{ team }-quarters-viz.csv"

    # Save the combined DataFrame to a new CSV file
    quarter_viz_df.to_csv(combined_directory + quarter_viz_filename, index=False)

    print("Save combined file " + quarter_viz_filename) 


# Takes 3-letter team name and season as string (ex: 23-24 is "2024")
scrape("MIA", "2022")