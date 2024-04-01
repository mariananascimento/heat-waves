import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import time

def get_urls(team="MIA", season="2024"):

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


def scrape_raw_pbp(url, filename):

    raw_directory = "data/raw/"

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

            # Process the table headers
            headers = [th.getText() for th in table.find_all('tr')[0].find_all(['th', 'td'])]

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

def clean_pbp(filename):

    raw_directory = "data/raw/"
    clean_directory = "data/clean/"

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

    # Overwrite the original file with the filtered data
    df.to_csv(clean_directory + filename, index=False)

    # Display the message
    print(f"The file '{filename}' has been written with the filtered data.")

urls = get_urls()
counter = 0

for url in urls:

    time.sleep(5)

    print(f"Iteration {counter}")
    counter = counter + 1

    # Extract the filename form the URL
    filename = url.split('/')[-1].replace('.html', '.csv')

    # Run the functions
    scrape_raw_pbp(url, filename)
    clean_pbp(filename)

    
