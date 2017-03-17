#!/bin/bash
volttron-ctl stop --tag ess
volttron-ctl remove --tag ess
volttron-pkg package ./services/contrib/ESSAgent
volttron-ctl install --tag ess ~/.volttron/packaged/essagent-1.0-py2-none-any.whl
#volttron-ctl config list   
volttron-ctl start --tag ess
volttron-ctl status
