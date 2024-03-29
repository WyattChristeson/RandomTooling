#!/usr/bin/python3
import requests
from bs4 import BeautifulSoup
import csv

def get_state_urls():
    url = "https://stateparks.com/index.html"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    state_urls = {}

    # Look for all links within the footer container which contains state links
    footer_container = soup.find("div", id="footer_leftX")
    if footer_container:
        links = footer_container.find_all("a")
        for link in links:
            if 'href' in link.attrs:
                # Process the text to extract just the state name
                state_name = link.text.strip().replace("State of ", "").replace(" Parks", "").strip()
                state_url = f"https://stateparks.com/{link['href']}"
                state_urls[state_name] = state_url
                #print(f"Found: {state_name} - {state_url}")  # For debugging

    return state_urls



def get_park_names_and_types(state_url):
    page = requests.get(state_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    park_names_types = []

    current_park_type = "Unknown"  # Default if no park type is found
    # Iterate through all elements in the park listings
    for element in soup.select("#parklistings div"):
        # Check if the element is a park type specifier
        if 'class' in element.attrs and 'parkType' in element['class']:
            current_park_type = element.text.strip()  # Update the current park type

        # Check if the element is a park link
        elif element.get('id') == 'parklink':
            park = element.find("a")
            if park:
                park_name = park.text.strip()
                park_names_types.append((park_name, current_park_type))

    return park_names_types


def get_address_places(park_name, state):
    # Base URL for Google Places API Text Search
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    
    # Full query with park name and state
    query = f"{park_name}, {state}"
    
    # Parameters for the API request
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "formatted_address,name",
        "key": "key-goes-here"  # Replace with your actual Google Places API key
    }
    
    # Send a request to the Places API
    response = requests.get(base_url, params=params)

    # Parse the response
    if response.status_code == 200:
        data = response.json()

        # Check if results were found
        if data['candidates']:
            first_result = data['candidates'][0]
            address = first_result.get('formatted_address', "No address found")
            return address
        else:
            return "No address found for this park."
    else:
        return "Failed to connect to the API."


def main():
    # Read the list of states from the file
    with open('states.txt', 'r') as file:
        states = file.read().splitlines()

    # Iterate over each state and process it
    for state_name in states:
        state_urls = get_state_urls()
        if state_name in state_urls:
            state_url = state_urls[state_name]
            park_names_types = get_park_names_and_types(state_url)

            parks_data = []

            for park_name, park_type in park_names_types:
                address = get_address_places(park_name, state_name)  # Using Google Places API for address
                parks_data.append({'Name': park_name, 'Address': address, 'Type': park_type})

            # Define the CSV file name dynamically based on the state name
            csv_file_name = f"{state_name.replace(' ', '_')}_parks.csv"

            with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=['Name', 'Address', 'Type'])
                writer.writeheader()
                for park in parks_data:
                    writer.writerow(park)

            print(f"CSV file has been created with the name {csv_file_name}")
        else:
            print(f"No URL found for {state_name}. Please check the state name or URL structure.")

if __name__ == "__main__":
    main()

