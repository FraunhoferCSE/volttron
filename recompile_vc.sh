#!//bin/bash
# Script for recompiling the React UI for Voltron
#
# Needs to run on the host, as volttron.
#
#  pushd /home/volttron/volttron/services/core/VolttronCentral
#git pull
#  volttron-ctl stop --tag vc &&  volttron-ctl remove --tag vc &
#  npm run build
#  #-development
#  nodejs node_modules/gulp/bin/gulp.js & 
#  FOO_PID=$!
#  sleep 10 &&  kill -TERM $FOO_PID
#  volttron-pkg package . && volttron-pkg configure /home/volttron/.volttron/packaged/volttroncentralagent-3.6.0-py2-none-any.whl config 
#  volttron-ctl install ~/.volttron/packaged/volttroncentralagent-3.6.0-py2-none-any.whl --tag vc
#  volttron-ctl start --tag vc
#  volttron-ctl restart --tag vcp
#  popd
#!/bin/bash

volttron-ctl stop --tag vc 
volttron-ctl remove --tag vc
volttron-pkg package ./services/core/VolttronCentral 
volttron-pkg configure config .services/core/VolttronCentral/config
volttron-ctl install --tag vc ~/.volttron/packaged/volttroncentralagent-4.0-py2-none-any.whl

#volttron-ctl config delete volttron.central --all
#volttron-ctl config store volttron.central config ./services/core/VolttronCentral/config
#volttron-ctl config list volttron.central
volttron-ctl start --debug --tag vc
volttron-ctl restart --debug --tag vcp
volttron-ctl status
