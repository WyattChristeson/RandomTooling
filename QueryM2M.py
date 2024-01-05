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
m2m_threshold_entries = defaultdict(Counter)
widget_types = defaultdict(lambda: defaultdict(lambda: defaultdict(str)))
query_sources = defaultdict(str)
timestamp_count = Counter()
earliest_timestamp = None
latest_timestamp = None
total_slow_queries = 0
total_queries = 0
total_duration = 0
max_values = defaultdict(lambda: defaultdict(lambda: {'translationDuration': 0, 'dataSourceExecuteDuration': 0, 'throttlingTimeWaiting': 0}))

def parse_log_line(line):
    pattern = r'"([^"]+)"\s*:\s*(?:"([^"]+)"|(\b\d+\b))'
    matches = re.findall(pattern, line)
    result = {}
    for key, str_val, num_val in matches:
        result[key] = str_val if str_val else num_val
    return result

def process_log_line_for_m2m(entry):
    m2m_flag = entry.get('m2mThresholdFlag\\', '').replace('\\', '').replace("'", "")
    if m2m_flag == '1':
        cube_name = entry.get('cubeName', 'No CubeName').strip("\\").strip("'").strip('"')
        dashboard = entry.get('dashboard', 'No Dashboard').strip("\\").strip("'").strip('"')
        widget = entry.get('widget', 'No Widget').strip("\\").strip("'").strip('"')
        widgetType = entry.get('widgetType', 'No Widget').strip("\\").strip("'").strip('"')

        # Increment the count for the dashboard/widget combination
        m2m_threshold_entries[(dashboard, widget, widgetType)].update([cube_name])

def is_valid_duration(entry):
    try:
        float_duration = float(entry.get('duration', '0').replace('"', ''))
        return float_duration > slow_query_threshold
    except ValueError:
        return False

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
        p95_index = ceil(count * 0.95) - 1
        stats['p95_duration'] = sorted_durations[p95_index]
    return stats

def parse_timestamp(timestamp_str):
    return datetime.fromisoformat(timestamp_str.replace('Z', '')).replace(tzinfo=None) #fromisoformat was introduced in Python 3.7, and will cause issues in old clients

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
        return 

    if duration > slow_query_threshold:
        cube_name = entry['cubeName']
        dashboard = entry.get('dashboard', 'No Dashboard')
        data[cube_name].append(entry)
        widgetType = entry.get('widgetType', 'No WidgetType').strip("\\").strip("'").strip('"')
        querySource = entry.get('querySource', 'No QuerySource').strip("\\").strip("'").strip('"')
        dashboard_widget_count[cube_name][dashboard][entry.get('widget', 'No Widget')] += 1
        widget_types[cube_name][dashboard][entry.get('widget', 'No Widget')] = widgetType
        query_sources[cube_name] = querySource
        timestamp_count[timestamp.strftime('%Y-%m-%d %H:%M')] += 1
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
                            continue

                        total_queries += 1
                        total_duration += duration
                        update_timestamp_range(timestamp)
                        process_log_line_for_m2m(entry)

                        if duration > slow_query_threshold:
                            process_slow_query(entry, timestamp)

                except KeyError as e:
                    continue



# Sort cube names based on the count of slow queries
sorted_cubes = sorted(data.items(), key=lambda x: calculate_stats(x[1])['count'])

# Loop to display the nested results in sorted order
for cube_name, entries in sorted_cubes:
    stats = calculate_stats(entries)
    querySource = query_sources[cube_name]
    print(f"\nCubeName: {cube_name} (Query Source: {querySource})")
    print(f"Count of Slow Queries: {stats['count']}")
    print(f"Average Duration: {stats['average_duration']:.3f}")
    print(f"Slowest Duration: {stats['max_duration']:.3f}")
    if 'p50_duration' in stats:
        print(f"P50 Duration: {stats['p50_duration']:.3f}")
    if 'p95_duration' in stats:
        print(f"P95 Duration: {stats['p95_duration']:.3f}")
    print(f"Maximum Concurrent Queries: {stats['max_concurrent_query']}")

    for dashboard, widgets in dashboard_widget_count[cube_name].items():
        max_vals = max_values[cube_name][dashboard]
        print(f"  Dashboard: {dashboard}")
        print(f"    Max Translation Duration: {max_vals['translationDuration']}")
        print(f"    Max Data Source Execution Duration: {max_vals['dataSourceExecuteDuration']}")
        print(f"    Max Throttling Time Waiting: {max_vals['throttlingTimeWaiting']}")
        for widget, freq in widgets.items():
            if freq > repeat_offender_threshold:
                widgetType = widget_types[cube_name][dashboard][widget]
                print(f"    Widget: {widget} (Type: {widgetType}) - Count: {freq}")

# Calculate the percentage of slow queries
if total_queries > 0:
    slow_queries_percentage = (total_slow_queries / total_queries) * 100
else:
    slow_queries_percentage = 0

overall_average_duration = total_duration / total_queries if total_queries else 0

# Display the result
day_suffix = "day" if days_to_look_back == 1 else "days"
print(f"\nFound {total_slow_queries} slow queries (duration > {slow_query_threshold} seconds) which is {slow_queries_percentage:.4f}% of {total_queries} total queries over the past {days_to_look_back} {day_suffix}. The overall average response time is {overall_average_duration:.3f} seconds")

print("\nSummary of detected possible M2Ms based on m2mThresholdFlag:")
for (dashboard, widget, widgetType), cube_names in m2m_threshold_entries.items():
    for cube_name, count in cube_names.items():
        print(f"Dashboard: {dashboard}, Widget: {widget}, Widget Type: {widgetType}, Cube: {cube_name} - Count: {count}")

if earliest_timestamp and latest_timestamp:
    print(f"\nTimestamp range of processed data: {earliest_timestamp} to {latest_timestamp}")
else:
    print("\nNo data available in the specified date range.")

print("\nTimestamps with Reported Slow Queries:")
sorted_timestamps = sorted(timestamp_count.items())
for timestamp, count in sorted_timestamps:
    print(f"  {timestamp}: {count} slow queries")
