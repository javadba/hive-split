hive-split
==========

Fix the Hive split udf to return trailing empty fields

This addresses Hive JIRA 5506     https://issues.apache.org/jira/browse/HIVE-5506.

The usage is same as the existing "split" except it has optional "limit" parameter that is forwarded to a slightly modified "split" method.

When the limit is not provided  or a positive number then any trailing empty/null values from the split ARE returned - which is the subject of the JIRA.
If the limit is set to 0 then the old behavior of truncation is applied.


Usage:


hive> add jar /path/to/clone/of/hive-split/UDFSplit.jar;
Added /shared/hive-split/UDFSplit.jar to class path
Added resource: /shared/hive-split/UDFSplit.jar
hive> create temporary function split as "org.apache.hadoop.hive.ql.udf.generic.UDFSplitWithLimit";
OK

# New behavior : return empty trailing fields
hive> select split("a\tb\tc\t\td\t\t","\t") from dual;
["a","b","c","","d","",""]
Time taken: 6.731 seconds, Fetched: 1 row(s)

# New behavior: supply a positive valued limit as third param
hive> select split("a\tb\tc\t\td\t\t","\t",999) from dual;
["a","b","c","","d","",""]

#  OLD behavior by setting third param (limit) to 0
hive> select split("a\tb\tc\t\td\t\t","\t",0) from dual;
["a","b","c","","d"]

