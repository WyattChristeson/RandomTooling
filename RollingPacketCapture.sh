nohup sudo tcpdump host 10.10.10.10 -i ens5 -vvv -tttt -S -G 3600 -W 24 -w /opt/tcpdump/capture-`date +%y_%m_%d_%H_%M_%S`.pcap
#! /bin/bash
for i in $(sudo find /opt/tcpdump -name "*.pcap"| head -n 1 ); do (sudo tar -czf $i.tgz $i && sudo rm -f $i); done
for i in $(sudo find /opt/tcpdump -mmin +1439 -name "*.tgz"| head -n 1); do (sudo echo "removing "$i | sudo tee capture.log && sudo rm -f $i); done
#! /bin/bash
sudo tcpdump host 10.10.10.10 -i ens5 -vvv -tttt -S -G 3600 -w /opt/tcpdump/capture-`date +%y_%m_%d_%H_%M_%S`.pcap
