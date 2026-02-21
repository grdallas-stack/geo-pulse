#!/bin/bash
cd /Users/garrettdallas/geo_pulse
source .env
python run_pipeline.py
git add data/
git commit -m "pipeline refresh $(date '+%Y-%m-%d %H:%M')"
git push
