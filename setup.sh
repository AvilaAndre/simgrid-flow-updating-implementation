# /bin/bash

echo "This script will setup a python virtual environment and install the requirements..."
echo

python3 -m venv .venv

echo "export LD_LIBRARY_PATH=$(pwd)/.venv/lib/python3.10/site-packages/:$LD_LIBRARY_PATH" >> .venv/bin/activate

source .venv/bin/activate

pip install -r requirements.txt

echo
echo "Setup finished, run \`source .venv/bin/activate\` to use the new virtual environment."
