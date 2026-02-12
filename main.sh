#!/bin/sh

echo "Upkeep Work Order Downloader"
printf 'Enter Email: '
read email
printf 'Enter Password: '
# hide user input then read password
stty_previous=$(stty -g)
trap "stty ${stty_previous}" EXIT  # restore on error
stty -echo  # disable echo
read password
stty echo  # restore echo
echo ""

# use email + password to get upkeep token
# token is needed to retrieve data from API
echo "Requesting Upkeep Auth Token..."
curl "https://api.onupkeep.com/api/v2/auth" \
  -X POST \
  -d email="$email" \
  -d password="$password" \
  | python main.py
