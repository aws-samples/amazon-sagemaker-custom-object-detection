#!/bin/bash
echo "Installing required modules."
sudo pip install -r requirements.txt -t .
echo "Removing test folders to lower overall size of Lambda deployment."
sudo find . -name "tests" -type d -exec rm -r "{}" \;
exit 0