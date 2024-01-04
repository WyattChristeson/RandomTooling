#!/usr/bin/env python3
import subprocess

output = subprocess.check_output(['kubectl', 'get', 'nodes', '-A', '--show-labels'])

nodes = {}
for line in output.decode().split('\n')[1:-1]:
    fields = line.split()
    node_name = fields[0]
    labels = fields[5].split(',')
    roles = set()
    for label in labels:
        key, value = label.split('=')
        if key.startswith('node-sisense'):
            roles.add(key.split('-')[2].lower())
    nodes[node_name] = roles

output_str = '  kubernetesNodes:\n'
for node_name, roles in nodes.items():
    roles_str = ', '.join(sorted(roles))
    output_str += f'    - {{ node: {node_name}, roles: "{roles_str}" }}\n'

print(output_str)
