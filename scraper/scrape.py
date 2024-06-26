import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import time
import os

team_location = ""
teams = False

def scrape(team, season):
    global teams, team_location

    # TODO: get list of teams for every year
    teams = pd.read_csv("data/teams/teams-2024.csv")

    team_location = teams[teams['code'] == team]['location'].iloc[0]

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
            clean_pbp(filename, team, season)
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
    global team_location

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

    # Initialize columns 'TeamScore', 'OpponentScore', and 'PointDifference'
    df['TeamScore'] = 0
    df['OpponentScore'] = 0
    df['PointDifference'] = 0

    # Find the column index for team in the DataFrame
    team_index = df.columns.get_loc(team_location)

    # Determine the indices for team and the opponent based on the column index of team
    if team_index == 1:
        team = 0
        opponent = 1
    elif team_index == 3:
        team = 1
        opponent = 0

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Split the 'Score' column value into two parts using '-' as the separator
        scores = row['Score'].split('-')
        # Convert the split values to integers immediately
        team_score = int(scores[team])
        opponent_score = int(scores[opponent])

        # Assign the converted scores to their respective columns
        df.at[index, 'TeamScore'] = team_score
        df.at[index, 'OpponentScore'] = opponent_score
        # Calculate and assign the point difference
        df.at[index, 'PointDifference'] = team_score - opponent_score

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
            elapsed_quarters = row['Quarter'] * quarter_seconds
        else:
            ots = row['Quarter'] - 4
            elapsed_quarters = (4 * quarter_seconds) + (ots * ot_seconds)

        elapsed_time = elapsed_quarters - remaining_time_in_quarter

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
    global teams, team_location

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
        if df.columns[1] == team_location:
            # Rename 2nd column to "Notes" and combine it with the 4th column
            df['Notes'] = df.iloc[:, 1].combine_first(df.iloc[:, 3])
            # Create new column "OpponentName" with the name of the 4th column
            df['OpponentName'] = df.columns[3]
            # Delete the 4th column
            df.drop(df.columns[[1, 3]], axis=1, inplace=True)

        elif df.columns[3] == team_location:
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

    # Create minial pbp dataset for viz

    # Create a DataFrame with the last row of each unique "id"
    last_per_id = combined_df.drop_duplicates(subset='id', keep='last')

    # Add all rows with unique "Score" for each unique "id"
    unique_scores_per_id = combined_df.drop_duplicates(subset=['id', 'Score'])

    # Combining the two DataFrames, removing duplicates again in case of overlap
    combined_df_viz = pd.concat([last_per_id, unique_scores_per_id]).drop_duplicates()

    # Remove columns: "Time", "Score", "Quarter", "Notes"
    combined_df_viz = combined_df_viz.drop(columns=['Time', 'Score', 'Quarter', 'Notes'])

    # Sort by "id" and then by "ElapsedTime"
    combined_df_viz = combined_df_viz.sort_values(by=['id', 'ElapsedTime'])

    # Replace "Tie" with "T" and "Lead Change" with "LC" in the "Event" column
    combined_df_viz['Event'] = combined_df_viz['Event'].replace({'Tie': 'T', 'Lead change': 'LC'})

    # Find the index of the last occurrence of each unique 'id'
    last_rows = combined_df_viz.groupby('id').tail(1).index

    # Mark the final socre with an F in the event column
    combined_df_viz.loc[last_rows, 'Event'] = 'F'

    # Create a dictionary mapping from the 'location' to 'code'
    location_to_code = teams.set_index('location')['code'].to_dict()

    # Replace values in the "Opponent" column using the map
    combined_df_viz['OpponentName'] = combined_df_viz['OpponentName'].map(location_to_code)

    # Rename the columns to better fit JS
    combined_df_viz = combined_df_viz.rename(columns={
        'Event': 'event',
        'TeamScore': 'TeamScore',
        'OpponentScore': 'opponentScore',
        'PointDifference': 'pointDifference',
        'ElapsedTime': 'elapsedTime',
        'OpponentName': 'opponent',
    })

    viz_filename = f"{ season }-{ team }-viz.csv"

    # Save the combined DataFrame to a new CSV file
    combined_df_viz.to_csv(combined_directory + viz_filename, index=False)

    print("Save combined file " + viz_filename) 

    # Filter for rows where Time is '12:00.0', then drop duplicates based on 'id' and 'Quarter'
    time_12_df = combined_df[combined_df['Time'] == '12:00.0'].drop_duplicates(['id', 'Quarter'])

    # Similarly, for '0:00.0'
    time_0_df = combined_df[combined_df['Time'] == '0:00.0'].drop_duplicates(['id', 'Quarter'])

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

    # Create quarter and mid-quarter version

    # Ensure the DataFrame is sorted by 'id', 'Quarter', and 'ElapsedTime' or 'Time' in descending order so that time counts down
    combined_df = combined_df.sort_values(by=['id', 'Quarter', 'Time'], ascending=[True, True, False])

    # Function to capture the score right before the first "5:" mark in each quarter
    def find_pre_five_minute_mark(group):
        # Find the index of the first occurrence where Time starts with "5:"
        first_five_idx = group[group['Time'].astype(str).str.startswith("5:")].index.min()
        # Select the row immediately before this index, if it exists
        if pd.notnull(first_five_idx):
            pre_five_idx = group.index[group.index.get_loc(first_five_idx) - 1]
            return group.loc[pre_five_idx]

    # Apply the function to each group
    pre_five_scores = combined_df.groupby(['id', 'Quarter'], group_keys=False).apply(find_pre_five_minute_mark).reset_index(drop=True)

    # Now you can concatenate this with your existing data frames for start and end times
    quarter_midquarter_combined_df = pd.concat([time_12_df, pre_five_scores, last_time_0_df]).drop_duplicates().reset_index(drop=True)

    # Ensure the combined data is sorted correctly
    quarter_midquarter_combined_df = quarter_midquarter_combined_df.sort_values(by=['id', 'Quarter', 'Time'], ascending=[True, True, False]).reset_index(drop=True)

    # Filter by Quarter scores only
    quarter_midquarter_filename = f"{ season }-{ team }-quarters-midquarters.csv"

    # Save the combined DataFrame to a new CSV file
    quarter_midquarter_combined_df.to_csv(combined_directory + quarter_midquarter_filename, index=False)

    print("Save combined file " + quarter_midquarter_filename) 

    # Create viz-friendly dataset with quarters and midquarter point differences

    quarter_midquarter_viz_combined_df = quarter_midquarter_combined_df.sort_values(by=['ElapsedTime']) \
        .groupby(['id', 'OpponentName'])['PointDifference'] \
        .apply(lambda x: ','.join(x.astype(str))) \
        .reset_index(name='Diffs')
    
    # Filter by Quarter scores only
    quarter_midquarter_viz_filename = f"{ season }-{ team }-quarters-midquarters-viz.csv"

    # Save the combined DataFrame to a new CSV file
    quarter_midquarter_viz_combined_df.to_csv(combined_directory + quarter_midquarter_viz_filename, index=False)

    print("Save combined file " + quarter_midquarter_viz_filename) 

# Takes 3-letter team name and season as string (ex: 23-24 is "2024")
scrape("MIA", "2024")

# remaining_teams = ['ATL', 'NJN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIL', 'MIN', 'NOH', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']

# for team_code in remaining_teams:
#     scrape(team_code, "2024")
  
