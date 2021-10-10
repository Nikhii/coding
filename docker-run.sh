#!/bin/sh
docker run  -v $(pwd):/app data-process:1.0.1 --inputfile donors.csv