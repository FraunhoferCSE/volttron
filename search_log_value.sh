#!/bin/bash
#cat ~/volttronproj/logs/volttron.log | grep -o "CommSt':[ ]*[0-9]*"
tail -f ~/volttronproj/logs/volttron.log | grep -o "$1':[ ]*[0-9]*"

