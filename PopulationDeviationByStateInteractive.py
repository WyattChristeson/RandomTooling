import pandas as pd
import geopandas as gpd
import folium

def standardize_name(name):
    # Remove common suffixes and trim whitespace
    for suffix in [' County', ' Parish', ' Borough', ' Census Area', ' Municipality', ' city']:
        name = name.replace(suffix, '')
    return name.strip()

def color_scale(deviation):
    """Returns a color based on gender deviation."""
    if deviation > 30:
        return '#000080'  # Very Dark Blue
    elif deviation > 25:
        return '#0000FF'  # Dark Blue
    elif deviation > 15:
        return '#3333FF'  # Medium Blue
    elif deviation > 10:
        return '#6666FF'  # Lighter Blue
    elif deviation > 5:
        return '#9999FF'  # Very Light Blue
    elif deviation > 0:
        return '#CCCCFF'  # Lightest Blue
    elif deviation > -5:
        return '#FFCCCC'  # Lightest Pink
    elif deviation > -10:
        return '#FF99AA'  # Very Light Pink
    elif deviation > -15:
        return '#FF6699'  # Lighter Pink
    elif deviation > -20:
        return '#FF3399'  # Medium Pink
    else:
        return '#FF0066'  # Dark Pink

def create_gender_deviation_map_folium(csv_file_path, shapefile_path):
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
    merged_data = counties.set_index('NAME').join(data.set_index('CTYNAME'), how='inner')

    # Convert to GeoJSON for Folium
    merged_geojson = merged_data.to_json()

    # Create a map object centered on the US
    m = folium.Map(location=[37.0902, -95.7129], zoom_start=4)

    # Add the GeoJSON layer to the map
    folium.GeoJson(
        merged_geojson,
        name='Gender Deviation',
        style_function=lambda feature: {
            'fillColor': color_scale(feature['properties']['gender_deviation']),
            'color': 'black',
            'weight': 0.5,
            'fillOpacity': 0.7,
        },
        tooltip=folium.GeoJsonTooltip(fields=['NAMELSAD', 'gender_deviation'], aliases=['County', 'Gender Deviation (%)']),
    ).add_to(m)

    # Add layer control and display the map
    folium.LayerControl().add_to(m)
    return m


# Replace with your file paths
csv_file_path = 'PopulationByCounty.csv'
shapefile_path = 'cb_2022_us_county_20m.shp' # Download from US Census Bureau https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

map = create_gender_deviation_map_folium(csv_file_path, shapefile_path)
map.save('us_gender_deviation_map.html')  # Saves the map as an HTML file

