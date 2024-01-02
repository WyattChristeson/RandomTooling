#!/usr/bin/python3
import gzip
import re
import glob
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from math import ceil

# Parameters to configure
days_to_look_back = 1  # Modify this to change the range of days to look back
slow_query_threshold = 30.0 # Filter for queries which took longer than this value to process
repeat_offender_threshold = 1 # Filters the number of widgets returned to help identify poorly written widgets and make the output prettier

# Log files to process
log_directory = '/var/log/sisense/sisense'
log_files = glob.glob(f'{log_directory}/query.log') + glob.glob(f'{log_directory}/query*.log-*.gz')

# Script Variables Declared
earliest_date = datetime.now().replace(tzinfo=None) - timedelta(days=days_to_look_back)
data = defaultdict(list)
dashboard_widget_count = defaultdict(lambda: defaultdict(Counter))
timestamp_count = Counter()
m2m_threshold_entries = []
earliest_timestamp = None
latest_timestamp = None
total_slow_queries = 0
total_queries = 0
total_duration = 0
max_values = defaultdict(lambda: defaultdict(lambda: {'translationDuration': 0, 'dataSourceExecuteDuration': 0, 'throttlingTimeWaiting': 0}))

def parse_log_line(line):
    pattern = r'"(duration|translationDuration|dataSourceExecuteDuration|concurrentQuery|throttlingTimeWaiting|widget|dashboard|cubeName|querySource|Log_DateTime|JAQL Text)":\s*([^,}]+)'
    matches = re.findall(pattern, line)
    return {match[0]: match[1].strip('"') for match in matches}

def is_valid_duration(entry):
    try:
        # Try converting the duration to a float
        float_duration = float(entry.get('duration', '0').replace('"', ''))
        return float_duration > slow_query_threshold
    except ValueError:
        # If conversion fails, it's not a valid duration
        return False

def process_log_line_for_m2m(entry):
    try:
        if entry.get('m2mThresholdFlag') == '1':  # Ensure this is string comparison
            cube_name = entry.get('cubeName', 'No CubeName')
            dashboard = entry.get('dashboard', 'No Dashboard')
            widget = entry.get('widget', 'No Widget')
          m2m_threshold_entries.append((cube_name, dashboard, widget))
    except KeyError:
        pass  # Handle or log as needed

def calculate_stats(entries):
    durations = [float(entry['duration']) for entry in entries]
    count = len(entries)
    sorted_durations = sorted(durations)
    stats = {
        'count': count,
        'average_duration': sum(sorted_durations) / count if count else 0,
        'max_duration': max(sorted_durations) if sorted_durations else 0,
        'max_concurrent_query': max(int(entry['concurrentQuery']) for entry in entries) if entries else 0
    }
    if count >= 2:
        stats['p50_duration'] = sorted_durations[int(count * 0.5)]
        # Use ceil to ensure the P95 is never less than the median
        p95_index = ceil(count * 0.95) - 1
        stats['p95_duration'] = sorted_durations[p95_index]
    return stats

def parse_timestamp(timestamp_str):
    return datetime.fromisoformat(timestamp_str.replace('Z', '')).replace(tzinfo=None)

def update_max_values(cube_name, dashboard, entry):
    max_vals = max_values[cube_name][dashboard]
    max_vals['translationDuration'] = max(max_vals['translationDuration'], float(entry.get('translationDuration', 0)))
    max_vals['dataSourceExecuteDuration'] = max(max_vals['dataSourceExecuteDuration'], float(entry.get('dataSourceExecuteDuration', 0)))
    max_vals['throttlingTimeWaiting'] = max(max_vals['throttlingTimeWaiting'], float(entry.get('throttlingTimeWaiting', 0)))

def process_slow_query(entry, timestamp):
    global total_slow_queries
    try:
        duration = float(entry.get('duration', 0))
    except ValueError:
        print(f"Warning: Invalid duration value '{entry.get('duration')}' in entry: {entry}")
        return  # Skip this entry

    if duration > slow_query_threshold:
        cube_name = entry['cubeName']
        dashboard = entry.get('dashboard', 'No Dashboard')
        data[cube_name].append(entry)
        dashboard_widget_count[cube_name][dashboard][entry.get('widget', 'No Widget')] += 1
        timestamp_count[timestamp.strftime('%Y-%m-%d %H')] += 1
        total_slow_queries += 1
        update_max_values(cube_name, dashboard, entry)

def update_timestamp_range(timestamp):
    global earliest_timestamp, latest_timestamp
    if earliest_timestamp is None or timestamp < earliest_timestamp:
        earliest_timestamp = timestamp
    if latest_timestamp is None or timestamp > latest_timestamp:
        latest_timestamp = timestamp

# Loop to parse the logs
for log_file in log_files:
    with (gzip.open if log_file.endswith('.gz') else open)(log_file, 'rt') as file:
        for line in file:
            if '"Log_Message":"FinishQuery"' in line:
                try:
                    entry = parse_log_line(line)
                    timestamp = parse_timestamp(entry['Log_DateTime'])
                    if timestamp >= earliest_date:
                        duration_str = entry.get('duration', '0')
                        try:
                            duration = float(duration_str)
                        except ValueError:
                            # Skip malformed duration entries
                            continue

                        total_queries += 1
                        total_duration += duration
                        update_timestamp_range(timestamp)

                        if duration > slow_query_threshold:
                            process_slow_query(entry, timestamp)

                except KeyError as e:
                    # Skip lines with missing keys
                    continue



# Sort cube names based on the count of slow queries
sorted_cubes = sorted(data.items(), key=lambda x: calculate_stats(x[1])['count'])

# Loop to display the nested results in sorted order
for cube_name, entries in sorted_cubes:
    stats = calculate_stats(entries)
    print(f"\nCubeName: {cube_name}")
    print(f"Count of Slow Queries: {stats['count']}")
    print(f"Average Duration: {stats['average_duration']:.2f}")
    print(f"Slowest Duration: {stats['max_duration']:.2f}")
    if 'p50_duration' in stats:
        print(f"P50 Duration: {stats['p50_duration']:.2f}")
    if 'p95_duration' in stats:
        print(f"P95 Duration: {stats['p95_duration']:.2f}")
    print(f"Maximum Concurrent Queries: {stats['max_concurrent_query']}")

    for dashboard, widgets in dashboard_widget_count[cube_name].items():
        max_vals = max_values[cube_name][dashboard]
        print(f"  Dashboard: {dashboard}")
        print(f"    Max Translation Duration: {max_vals['translationDuration']}")
        print(f"    Max Data Source Execution Duration: {max_vals['dataSourceExecuteDuration']}")
        print(f"    Max Throttling Time Waiting: {max_vals['throttlingTimeWaiting']}")
        for widget, freq in widgets.items():
            if freq > repeat_offender_threshold:
                print(f"    Widget: {widget} - Count: {freq}")

# Calculate the percentage of slow queries
if total_queries > 0:
    slow_queries_percentage = (total_slow_queries / total_queries) * 100
else:
    slow_queries_percentage = 0

overall_average_duration = total_duration / total_queries if total_queries else 0

# Display the result
day_suffix = "day" if days_to_look_back == 1 else "days"
print(f"\nFound {total_slow_queries} slow queries (duration > {slow_query_threshold} seconds) which is {slow_queries_percentage:.4f}% of {total_queries} total queries over the past {days_to_look_back} {day_suffix}. The overall average response time is {overall_average_duration:.3f} seconds")

if earliest_timestamp and latest_timestamp:
    print(f"\nTimestamp range of processed data: {earliest_timestamp} to {latest_timestamp}")
else:
    print("\nNo data available in the specified date range.")

print("\nTimestamps with Reported Slow Queries:")
sorted_timestamps = sorted(timestamp_count.items())
for timestamp, count in sorted_timestamps:
    print(f"  {timestamp}: {count} slow queries")
