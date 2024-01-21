#!/usr/bin/python3
from lxml import etree as ET
import logging
import difflib

# Set up logging
log_file = 'kml_merger.log'
logging.basicConfig(level=logging.INFO, filename=log_file, filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

def parse_kml(file_path):
    """
    Parse a KML file and return the parsed tree and its namespace map.
    """
    logging.info(f"Parsing file: {file_path}")
    tree = ET.parse(file_path)
    return tree, tree.getroot().nsmap

def is_similar(placemark1, placemark2, nsmap):
    """
    Check if two placemarks are similar based on their names and addresses.
    """
    name1 = placemark1.find(f'.//{{{nsmap[None]}}}name').text or ''
    name2 = placemark2.find(f'.//{{{nsmap[None]}}}name').text or ''
    address1 = placemark1.find(f'.//{{{nsmap[None]}}}address').text or ''
    address2 = placemark2.find(f'.//{{{nsmap[None]}}}address').text or ''

    # Skip if address is set to name (indicating missing address)
    if name1 == address1 or name2 == address2:
        return False

    similarity_threshold = 0.8  # Adjust as needed
    name_similarity = difflib.SequenceMatcher(None, name1, name2).ratio()
    address_similarity = difflib.SequenceMatcher(None, address1, address2).ratio()

    # Require both name and address to be similar
    return name_similarity > similarity_threshold and address_similarity > similarity_threshold

def process_placemark(placemark, nsmap):
    """
    Process each placemark to ensure it has an address, and normalize the address if it does.
    """
    existing_address_elem = placemark.find(f'.//{{{nsmap[None]}}}address')
    if existing_address_elem is not None and existing_address_elem.text:
        address_text = existing_address_elem.text
        # Normalize address
        if "United States" in address_text:
            existing_address_elem.text = address_text.replace(", United States", "")
        elif "USA" in address_text:
            existing_address_elem.text = address_text.replace(", USA", "")
    else:
        # Set address to the name of the placemark if address is missing
        placemark_name = placemark.find(f'.//{{{nsmap[None]}}}name').text
        if placemark_name:
            if existing_address_elem is None:
                existing_address_elem = ET.SubElement(placemark, f'{{{nsmap[None]}}}address')
            existing_address_elem.text = placemark_name

def combine_placemark_data(existing_placemark, new_placemark, nsmap):
    """
    Combine data from two placemarks with the same name, ensuring no information is lost.
    """
    def get_or_create_element(parent, tag):
        element = parent.find(f'.//{{{nsmap[None]}}}{tag}')
        if element is None:
            element = ET.SubElement(parent, f'{{{nsmap[None]}}}{tag}')
        return element

    # Merge description
    existing_desc = get_or_create_element(existing_placemark, 'description')
    new_desc = new_placemark.find(f'.//{{{nsmap[None]}}}description')
    if new_desc is not None and new_desc.text and (new_desc.text not in (existing_desc.text or '')):
        existing_desc.text = (existing_desc.text or '') + ' ' + new_desc.text

    # Normalize and merge address
    existing_address_elem = get_or_create_element(existing_placemark, 'address')
    new_address_elem = new_placemark.find(f'.//{{{nsmap[None]}}}address')
    if new_address_elem is not None and new_address_elem.text:
        address_text = new_address_elem.text
        # Clean address if it contains 'United States' or 'USA'
        if "United States" in address_text:
            address_text = address_text.replace(", United States", "")
        elif "USA" in address_text:
            address_text = address_text.replace(", USA", "")
        if address_text not in (existing_address_elem.text or ''):
            existing_address_elem.text = (existing_address_elem.text or '') + ' ' + address_text
    elif existing_address_elem.text is None or existing_address_elem.text.strip() == '':
        # Set address to placemark's name if no address is found
        placemark_name = new_placemark.find(f'.//{{{nsmap[None]}}}name').text
        existing_address_elem.text = placemark_name
            # Process each placemark after merging data
    process_placemark(existing_placemark, nsmap)
    process_placemark(new_placemark, nsmap)

    # Merge ExtendedData
    existing_data = get_or_create_element(existing_placemark, 'ExtendedData')
    new_data = new_placemark.find(f'.//{{{nsmap[None]}}}ExtendedData')
    if new_data is not None:
        for new_data_elem in new_data.findall(f'.//{{{nsmap[None]}}}Data'):
            new_data_name = new_data_elem.get('name')
            existing_data_elem = existing_data.find(f'.//{{{nsmap[None]}}}Data[@name="{new_data_name}"]')
            if existing_data_elem is None:
                existing_data.append(new_data_elem)
            else:
                existing_value_elem = get_or_create_element(existing_data_elem, 'value')
                new_value_elem = new_data_elem.find(f'.//{{{nsmap[None]}}}value')
                if new_value_elem.text and new_value_elem.text not in (existing_value_elem.text or ''):
                    existing_value_elem.text = (existing_value_elem.text or '') + ' ' + new_value_elem.text


def merge_kml_trees(trees):
    """
    Merge multiple KML trees by adding or combining 'Placemark' elements.
    """
    merged_tree = trees[0][0]
    merged_nsmap = trees[0][1]
    merged_document = merged_tree.getroot().find(f'.//{{{merged_nsmap[None]}}}Document')
    placemark_dict = {}

    for tree, nsmap in trees:
        document = tree.getroot().find(f'.//{{{nsmap[None]}}}Document')
        if document is not None:
            for new_placemark in document.findall(f'.//{{{nsmap[None]}}}Placemark'):
                process_placemark(new_placemark, nsmap)

                # Check for similar placemark
                similar_placemark = None
                for existing_name, existing_placemark in placemark_dict.items():
                    if is_similar(existing_placemark, new_placemark, nsmap):
                        similar_placemark = existing_placemark
                        break

                if similar_placemark:
                    combine_placemark_data(similar_placemark, new_placemark, nsmap)
                else:
                    new_name = new_placemark.find(f'.//{{{nsmap[None]}}}name').text
                    merged_document.append(new_placemark)
                    placemark_dict[new_name] = new_placemark

    return merged_tree

def split_and_save_kml(tree, output_file_path, max_features=2000):
    """
    Split the KML tree into multiple files, each containing up to max_features placemarks.
    """
    root = tree.getroot()
    nsmap = root.nsmap
    document = root.find(f'.//{{{nsmap[None]}}}Document')
    placemarks = document.findall(f'.//{{{nsmap[None]}}}Placemark')

    for i in range(0, len(placemarks), max_features):
        new_root = ET.Element(root.tag, nsmap=root.nsmap)
        new_document = ET.SubElement(new_root, document.tag, nsmap=document.nsmap)
        new_document.extend(placemarks[i:i + max_features])
        new_tree = ET.ElementTree(new_root)
        part_file_path = f"{output_file_path.rsplit('.', 1)[0]}_part{i // max_features + 1}.kml"
        with open(part_file_path, 'wb') as file:
            new_tree.write(file, xml_declaration=True, encoding='UTF-8', pretty_print=True)
        logging.info(f"Saved {part_file_path} with {len(placemarks[i:i + max_features])} placemarks")

def merge_kml_files(file_paths, output_file_path):
    """
    Merge multiple KML files and save the result to a specified location.
    """
    trees = [parse_kml(file_path) for file_path in file_paths]
    merged_tree = merge_kml_trees(trees)
    split_and_save_kml(merged_tree, output_file_path)

# Example usage
file_paths = ['AllTheThings.kml', 'StateParks.kml', 'NationalMonuments.kml', 'RecreationAreas.kml']
output_file_path = 'CombinedKML.kml'

merge_kml_files(file_paths, output_file_path)
