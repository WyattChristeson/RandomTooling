#Calculate performance statistics from finished queries, run from any SSH session to a node where logs are stored
Filepath=/var/log/sisense/sisense/ && { cat ${Filepath}query.log; zcat ${Filepath}query*.log-*.gz 2>/dev/null; } | awk -F'[,:]' '
    function percentile(arr, p, n) {
        if (n == 1) return arr[1];

        idx = 1 + (n - 1) * p;
        idx_int = int(idx);
        idx_frac = idx - idx_int;

        if (idx_int >= n) return arr[n];
        return arr[idx_int] * (1 - idx_frac) + arr[idx_int + 1] * idx_frac;
    }

    BEGIN {
        totalCount = 0;
    }
    /FinishQuery/ {
        delete data;
        for (i=1; i<=NF; i++) {
            if ($i ~ /"duration"|"cubeName"/) {
                key = $i;
                value = $(i+1);
                gsub(/"/, "", key);
                gsub(/"/, "", value);
                data[key] = value;
            }
        }
        duration = data["duration"] + 0;
        cube = data["cubeName"];
        allDurations[++totalCount] = duration;
        cubeDurations[cube][++cubeCounts[cube]] = duration;
    }
    END {
        # Calculate and store overall statistics
        asort(allDurations);
        overallStats["Total Queries Processed"] = totalCount;
        overallStats["P50"] = percentile(allDurations, 0.5, totalCount);
        overallStats["P90"] = percentile(allDurations, 0.9, totalCount);
        overallStats["P95"] = percentile(allDurations, 0.95, totalCount);
        overallStats["P99"] = percentile(allDurations, 0.99, totalCount);

        # Print header
        printf "%-30s %-40s %-40s %-40s %-40s %-30s\n\n", "Data Model Name", "Total Queries Processed", "Average Duration in seconds (P50)", "90th Percentile (P90)", "95th Percentile (P95)", "99th Percentile (P99)";

        # Print statistics for each cubeName
        for (cube in cubeDurations) {
            n = cubeCounts[cube];
            asort(cubeDurations[cube]);
            printf "%-30s %-40d %-40.3f %-40.3f %-40.3f %-40.3f\n", cube, n, percentile(cubeDurations[cube], 0.5, n), percentile(cubeDurations[cube], 0.9, n), percentile(cubeDurations[cube], 0.95, n), percentile(cubeDurations[cube], 0.99, n);
        }

         # Print overall statistics as a line item
        printf "%-30s %-40d %-40.3f %-40.3f %-40.3f %-40.3f\n", "Overall Query Performance", overallStats["Total Queries Processed"], overallStats["P50"], overallStats["P90"], overallStats["P95"], overallStats["P99"];
    }
'

