# CDH_jmx
A python script to generate a *.tsv file from CDH exposed jmx values

Most Hadoop services expose jmx by appending /jmx to the UI.

For example, the DataNode UI is http://<fqdn>:20003.  
Jmx is exposed on http://<fqdn>:20003/jmx
