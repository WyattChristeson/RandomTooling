import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

def standardize_name(name):
    # Remove common suffixes and trim whitespace
    for suffix in [' County', ' Parish', ' Borough', ' Census Area', ' Municipality', ' city']:
        name = name.replace(suffix, '')
    return name.strip()

def create_gender_deviation_map(csv_file_path, shapefile_path):
    # Read the CSV file
    data = pd.read_csv(csv_file_path)

    # Calculate gender deviation
    data['gender_deviation'] = ((data['POPEST_MALE'] - data['POPEST_FEM']) / data['POPESTIMATE']) * 100

    # Load the shapefile
    counties = gpd.read_file(shapefile_path)

    # Reproject if necessary
    if counties.crs.to_string() != 'EPSG:4326':
        counties = counties.to_crs('EPSG:4326')

    # Standardize county names in both datasets
    data['CTYNAME'] = data['CTYNAME'].apply(standardize_name)
    counties['NAME'] = counties['NAME'].apply(standardize_name)

    # Merge the shapefile with the CSV data
    merged_data = counties.set_index('NAME').join(data.set_index('CTYNAME'), how='inner')  # Changed to 'inner' join

    # Plotting the map
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    merged_data.plot(column='gender_deviation', cmap='coolwarm', linewidth=0.8, ax=ax, edgecolor='0.8')

    # Adding a color bar
    plt.colorbar(merged_data.plot(column='gender_deviation', cmap='coolwarm', ax=ax).get_children()[1], ax=ax)

    # Title and labels
    ax.set_title('Gender Deviation Across US Counties', fontsize=15)
    ax.axis('off')

    plt.show()

# Replace with your file paths
csv_file_path = 'PopulationByCounty.csv'
shapefile_path = 'cb_2022_us_county_20m.shp' #Download from US Census Bureau https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

create_gender_deviation_map(csv_file_path, shapefile_path)

