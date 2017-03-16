#!/bin/bash
volttron-ctl stop --tag sqlh
volttron-ctl remove --tag sqlh
volttron-pkg package ./services/core/SQLHistorian
volttron-pkg configure sqlhistorianangent-3.6.1 ./services/core/SQLHistorian/config
volttron-ctl install --tag sqlh /home/sundial/.volttron/packaged/sqlhistorianagent-3.6.1-py2-none-any.whl --vip-identity platform.historian
volttron-ctl start --tag  sqlh
volttron-ctl status --tag sqlh
