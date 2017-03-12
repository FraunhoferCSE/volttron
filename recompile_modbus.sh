#!/bin/bash
volttron-ctl stop --tag mb
volttron-ctl remove --tag mb
volttron-pkg package ./services/core/MasterDriverAgent
volttron-ctl install --tag mb ~/.volttron/packaged/master_driveragent-3.1.1-py2-none-any.whl
volttron-ctl config delete platform.driver --all
volttron-ctl config store platform.driver config ./services/core/MasterDriverAgent/master-driver.agent
volttron-ctl config store platform.driver devices/Shirley-MA/South/PMC ./services/core/MasterDriverAgent/devices/shirley-ma-south-pmc
volttron-ctl config store platform.driver registry_configs/SOUTH.csv ./services/core/MasterDriverAgent/registry_configs/SOUTH.csv --csv
volttron-ctl config list platform.driver
volttron-ctl start --tag mb
volttron-ctl status
