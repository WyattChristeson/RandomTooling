#!/usr/bin/python3
from xml.etree import ElementTree as ET

def parse_kml(file_path):
    """
    Parse a KML file and return the parsed tree and its namespace.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    namespace = ''
    for elem in root.iter():
        if elem.tag.startswith('{'):
            uri, ignore, tag = elem.tag[1:].partition('}')
            if uri and not namespace:
                namespace = uri
            elem.tag = tag  # Strip the namespace
    return tree, namespace

def merge_kml_trees(tree1, tree2, namespace):
    """
    Merge two KML trees by adding 'Placemark' elements from the second tree to the first.
    """
    root1 = tree1.getroot()
    document1 = root1.find(f'{{{namespace}}}Document')

    for placemark in tree2.getroot().findall(f'./{{{namespace}}}Document/{{{namespace}}}Placemark'):
        document1.append(placemark)

    return tree1

def split_kml_tree(tree, namespace, max_layers, max_features, base_output_path):
    """
    Split a KML tree into multiple files based on layer and feature limits.
    """
    root = tree.getroot()
    document = root.find(f'{{{namespace}}}Document')
    if document is None:
        raise ValueError("No main 'Document' tag found in the KML tree")

    # Initialize counters and file index
    layer_count = 0
    feature_count = 0
    file_index = 1

    new_tree = ET.ElementTree(ET.Element(root.tag, root.attrib))
    new_document = ET.SubElement(new_tree.getroot(), document.tag, document.attrib)

    for elem in document:
        if elem.tag == 'Folder' or elem.tag == 'Document':
            if layer_count >= max_layers or feature_count >= max_features:
                # Save the current tree and start a new one
                new_tree.write(f'{base_output_path}_{file_index}.kml')
                file_index += 1
                new_tree = ET.ElementTree(ET.Element(root.tag, root.attrib))
                new_document = ET.SubElement(new_tree.getroot(), document.tag, document.attrib)
                layer_count = 0
                feature_count = 0

            new_document.append(elem)
            layer_count += 1
            feature_count += len(elem.findall(f'./{{{namespace}}}Placemark'))

        elif elem.tag == 'Placemark':
            if feature_count >= max_features:
                # Save the current tree and start a new one
                new_tree.write(f'{base_output_path}_{file_index}.kml')
                file_index += 1
                new_tree = ET.ElementTree(ET.Element(root.tag, root.attrib))
                new_document = ET.SubElement(new_tree.getroot(), document.tag, document.attrib)
                layer_count = 0
                feature_count = 0

            new_document.append(elem)
            feature_count += 1

    # Save the last tree if it has any content
    if new_document:
        new_tree.write(f'{base_output_path}_{file_index}.kml')

def merge_and_split_kml_files(file_path1, file_path2, max_layers, max_features, base_output_path):
    """
    Merge two KML files and then split the result based on layer and feature limits.
    """
    tree1, ns1 = parse_kml(file_path1)
    tree2, ns2 = parse_kml(file_path2)

    if ns1 != ns2:
        raise ValueError("The KML files have different namespaces and cannot be merged")

    merged_tree = merge_kml_trees(tree1, tree2, ns1)
    split_kml_tree(merged_tree, ns1, max_layers, max_features, base_output_path)

# Example usage
file_path1 = 'path_to_first_kml_file.kml'
file_path2 = 'path_to_second_kml_file.kml'
max_layers = 10  # Maximum layers per file
max_features = 2000  # Maximum features per file
base_output_path = 'path_to_output_files_base'  # Base path for output files

merge_and_split_kml_files(file_path1, file_path2, max_layers, max_features, base_output_path)
