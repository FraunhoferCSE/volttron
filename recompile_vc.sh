#!/bin/bash

volttron-ctl stop --tag vc 
volttron-ctl remove --tag vc
volttron-pkg package ./services/core/VolttronCentral 
volttron-ctl install --tag vc ~/.volttron/packaged/volttroncentralagent-4.0-py2-none-any.whl

volttron-ctl config delete volttron.central --all
volttron-ctl config store volttron.central config ./services/core/VolttronCentral/config
volttron-ctl config list volttron.central
volttron-ctl start --tag vc
volttron-ctl restart --tag vcp
volttron-ctl status
