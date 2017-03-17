#!/bin/bash
volttron-ctl stop --tag sitea
volttron-ctl remove --tag sitea
volttron-pkg package ./services/contrib/SiteAnalysisAgent
volttron-ctl install --tag sitea ~/.volttron/packaged/site_analysisagent-1.0-py2-none-any.whl
#volttron-ctl config list   sitea
volttron-ctl start --tag sitea
volttron-ctl status
