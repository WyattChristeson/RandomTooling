#Find OOM-Killed Containers on any given node and try to parse out the container ID, the individual thread that ran out of memory, the parent process that failed, and resolve the container ID to any present log files within the system.
#Run once per node in the environment, copy and paste into any root privileged Shell session to any Kubernetes worker node.
journalctl --since "3 days ago" | awk -v RS='\n' -v container_path="/var/log/containers/" '
    /kernel:.*invoked oom-killer/ {
        split($0, a, "kernel: ");
        split(a[2], b, " invoked oom-killer");
        process = b[1];
        getline;
    }
    /oom-kill/ && process {
        date = $1 " " $2 " " $3;
        gsub(/:[0-9]{2}$/, "", date);
        match($0, /task=([^,]+)/, task_match);
        task_name = task_match[1];
        match($0, /cpuset=([^,]+)/, cpuset_match);
        container_id = cpuset_match[1];
        
        if (task_name != "") {
            ls_cmd = "ls -l " container_path "*-" container_id ".log 2>/dev/null";
            ls_output = "";
            if ((ls_cmd | getline ls_output) > 0) {
                close(ls_cmd);
                if (split(ls_output, ls_array) >= 9) {
                    log_file = ls_array[9] " " ls_array[10] " " ls_array[11];
                    print date, task_name, process, "invoked oom-killer:", container_id, "->", log_file;
                } else {
                    print date, task_name, process, "invoked oom-killer:", container_id, "-> Log file not found";
                }
            } else {
                close(ls_cmd);
                print date, task_name, process, "invoked oom-killer:", container_id, "-> Log file not found";
            }
        }
        process = "";  # Reset process for next entry
    }'
