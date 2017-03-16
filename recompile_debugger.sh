#!/bin/bash
volttron-ctl stop --tag debugger
volttron-ctl remove --tag debugger
rm ~/.volttron/packaged/debugger-0.1-py2-none-any.whl

volttron-pkg package ./services/contrib/Debugger
volttron-ctl install --tag debugger ~/.volttron/packaged/debugger-0.1-py2-none-any.whl
#volttron-ctl config delete platform.driver --all
#volttron-ctl config store platform.driver config ./services/core/MasterDriverAgent/master-driver.agent
#volttron-ctl config store platform.driver devices/Shirley-MA/South/PMC ./services/core/MasterDriverAgent/devices/shirley-ma-south-pmc
#volttron-ctl config store platform.driver registry_configs/SOUTH.csv ./services/core/MasterDriverAgent/registry_configs/SOUTH.csv --csv
#volttron-ctl config list platform.driver
volttron-ctl start --tag debugger
volttron-ctl status
