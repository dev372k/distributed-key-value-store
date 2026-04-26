ips=($(cat ../public_ips.txt))

for ip in "${ips[@]}"; do
  echo "Requesting $ip after updating ..."
  curl --connect-timeout 5 --max-time 5 \
    "http://$ip:3030/get?key=x"
  echo
done