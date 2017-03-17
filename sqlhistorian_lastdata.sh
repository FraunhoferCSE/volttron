#!/bin/bash
echo "Last timestamped data:"
sqlite3 ~/.volttron/data/historian.sqlite <<EOL

select "Now :",datetime();

select "Last:",datetime(MAX(ts),'-1 seconds') from data;

select data.topic_id,data.value_string,topics.topic_name
from data, topics
where data.topic_id=topics.topic_id
and datetime(data.ts) >= (SELECT datetime(MAX(ts),'-1 seconds') from data);
EOL


