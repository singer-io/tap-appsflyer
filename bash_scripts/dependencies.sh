#!/bin/bash

python3 -m setup.py bdist_wheel
cd dist
python3 -m pip wheel -r ~/workspace/requirements.txt --no-dependencies
cp -r ~/workspace/dist ~/workspace/glue
rm -r ~/workspace/dist
mv ~/workspace/glue/dist ~/workspace/glue/dependencies


