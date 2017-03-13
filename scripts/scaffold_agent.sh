#!/bin/bash

refactor()
{
	local PATTERN=$1
	local REPLACEMENT=$2
	local DIR=${3:-.}

	(( $# < 2 )) && {
		echo "usage: ${0##*/} PATTERN REPLACEMENT [DIR]"
		return 1
	}

	# rename directories before files
	local TYPE REFACTORED
	for TYPE in d f
	do
		find "$DIR" -type "$TYPE" -name "*$PATTERN*" | while read -r
		do
			REFACTORED=${REPLY//$PATTERN/$REPLACEMENT}
			echo "renaming $REPLY to $REFACTORED"
			mv "$REPLY" "$REFACTORED"
		done
	done

	local TMP=".tmp-${0##*/}-$$"
	grep -rl "$PATTERN" "$DIR"/* | while read -r
	do
		echo "replacing pattern in $REPLY"
		sed -e "s/$PATTERN/$REPLACEMENT/g" < "$REPLY" > "$TMP" &&
			cat "$TMP" > "$REPLY"

		rm -f "$TMP"
	done
}

if [ $# -eq 0 ]
  then
    echo "usage: scaffold_agent.sh NewAgentName"
    exit
fi

fullname=$1
#firstletter="${fullname:0:1}"
lowername="${fullname,,}"

export SCAF_TEMP=$(mktemp -d /tmp/scaf-XXXXXX)
export SCAF_TARGET=$SCAF_TEMP
export SCAF_BASE=~/volttronproj/scripts/scaffolding

echo "Building scaffolded agent in $SCAF_TEMP ."
cp $SCAF_BASE/scaf_base/* $SCAF_TARGET -r

refactor ScaffoldAgent $fullname $SCAF_TARGET
refactor scaffoldagent $lowername $SCAF_TARGET

if [ ! -d ~/volttronproj/services/contrib/$fullname ]; then

  read -p "Copy scaffolded agent to services/contrib (y/n)? " -n 1 -r
  echo ""  
if [[ $REPLY =~ ^[Yy]$ ]]
  then
    cp $SCAF_TARGET/* ~/volttronproj/services/contrib/ -r
    echo "run ./services/contrib/$fullname/make-$lowername from volttron directory to build agent."
  else
       echo "Files in $SCAF_TARGET ."
  fi
else
   echo "Files in $SCAF_TARGET ."
fi


