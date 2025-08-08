#!/usr/bin/sh

if ! command -v python3
then
    echo -e "python3 is required, but seems to be missing"
    exit 1
fi

if [[ ! -d venv ]]
then
    python3 -m venv venv ||  { echo -e "virtual environment failed"; exit 1; }
fi

source venv/bin/activate
pip install --upgrade pip || { echo -e "pip command failed"; exit 1; }
pip install -r requirements.txt || { echo -e "pip command failed"; exit 1; }

echo -e "\nSetup complete"
