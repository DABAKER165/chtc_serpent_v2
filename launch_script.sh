#!/usr/bin/env bash
# examples on how to launch scripts
python3 ~/github/chtc_serpent_v2/main.py \
--config_dir ~/chtc_serpent/config \
--submission_dir ~/chtc_serpent/chtc_serpent_submissions

# examples on how to launch alert scripts
# this script is underdevelopment
python3 ~/github/chtc_serpent_v2/monitor_hardware.py \
--config_dir ~/chtc_serpent/config_test \
--submission_dir ~/chtc_serpent/chtc_serpent_submissions \
--alert_config ~/github/chtc_serpent_v2/serpent_configs/alert_limits.json \
--monitoring_dir ~/github/chtc_serpent_v2/serpent_submissions

