gnome-terminal -- /bin/sh -c 'python3 master.py config.json RANDOM'
gnome-terminal -- /bin/sh -c 'python3 worker.py 4000 1'
gnome-terminal -- /bin/sh -c 'python3 worker.py 4001 2'
gnome-terminal -- /bin/sh -c 'python3 worker.py 4002 3'
python3 requests.py 5
echo "All running"
