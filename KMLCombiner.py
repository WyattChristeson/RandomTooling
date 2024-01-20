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

def merge_kml_files(file_path1, file_path2, output_file_path):
    """
    Merge two KML files and save the result to a specified location.
    """
    tree1, ns1 = parse_kml(file_path1)
    tree2, ns2 = parse_kml(file_path2)

    if ns1 != ns2:
        raise ValueError("The KML files have different namespaces and cannot be merged")

    merged_tree = merge_kml_trees(tree1, tree2, ns1)
    merged_tree.write(output_file_path)

# Example usage
file_path1 = 'path_to_first_kml_file.kml'
file_path2 = 'path_to_second_kml_file.kml'
output_file_path = 'path_to_output_merged_file.kml'

merge_kml_files(file_path1, file_path2, output_file_path)
