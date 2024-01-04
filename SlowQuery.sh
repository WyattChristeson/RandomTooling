# Find slow queries! Anything that took more than 5 seconds, run from any SSH session to a node where logs are stored
Filepath=/var/log/sisense/sisense/ && { cat ${Filepath}query.log; zcat ${Filepath}query*.log-*.gz 2>/dev/null; } | awk -F'[,:]' '
    /FinishQuery/ {
        delete data;
        for (i=1; i<=NF; i++) {
            if ($i ~ /"duration"|"translationDuration"|"dataSourceExecuteDuration"|"concurrentQuery"|"throttlingTimeWaiting"|"widget"|"dashboard"|"cubeName"|"querySource"/) {
                key = $i;
                value = $(i+1);
                gsub(/"/, "", key);
                gsub(/"/, "", value);
                data[key] = value;
            }
        }
        if (data["duration"] + 0 > 5.000) {
            printf "{duration: %s}, ", data["duration"];
            printf "{translationDuration: %s}, ", data["translationDuration"];
            printf "{dataSourceExecuteDuration: %s}, ", data["dataSourceExecuteDuration"];
            printf "{concurrentQuery: %s}, ", data["concurrentQuery"];
            printf "{throttlingTimeWaiting: %s}, ", data["throttlingTimeWaiting"];
            if (data["widget"] != "") printf "{widget: %s}, ", data["widget"];
            if (data["dashboard"] != "") printf "{dashboard: %s}, ", data["dashboard"];
            if (data["cubeName"] != "") printf "{cubeName: %s}, ", data["cubeName"];
            printf "{querySource: %s}", data["querySource"];
            print "";
        }
    }
' | sort -t' ' -k2,2n
