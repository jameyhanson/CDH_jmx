'''
Created on Mar 27, 2017
Desc:  Convert *.json file exposed by Hadoop /jmx interface
       to *.csv file for easy review
Refs: json module https://docs.python.org/3/library/json.html
    csv module    https://docs.python.org/3/library/csv.html
@author: jphanso
'''

import json
import urllib.request
from sys import exit

def get_params():
    params = {}
    params['source_type'] = 'html'
    params['source_string'] = 'http://nightly511-unsecure-3.gce.cloudera.com:8042/jmx'
    params['service'] = 'NodeManager'
    params['out_file'] = 'D:/Users/jphanso/Documents/JMX/nm_jmx.tsv'
    return  params

def get_json_dict(source_type, source_string):
    if source_type == 'file':
        json_string = open(source_string, 'r')
    elif source_type == 'html':
        json_string = urllib.request.urlopen(source_string)
    else:
        print('Call with html or file')
        
        exit()
        
    json_dict = json.load(json_string)
    return json_dict

def write_line(line, out_file):
    # each line begins with 'service' and ends with 'parameter' \t 'value'
    #      columns between are the JSON hierarchy and are padded with tabs in
    #      the middle
    jmx_line = (line[0] + '\t'*(7 - len(line)) +
                "\t".join(map(str,line[1:])) + '\n')    
    out_file.write(jmx_line)

def main():
    params = get_params()
    json_dict = get_json_dict(params['source_type'], params['source_string'])
    
    out_file = open(params['out_file'], 'w')
    header = ('service\t' + 'key0\t' + 'key1\t' + 'key2\t' + 'key3\t'+ 'val2\n')
    out_file.write(header)

    for item in json_dict['beans']:
        # output three layers of nesting.  
        # from inner to outer is key/value0, key/value1 and key/value2

        for key0, value0 in item.items():
            if key0 == 'name':
            # key = name is reserved and used for a column
                param_group = str(value0)
                
            if type(value0) == dict:
                for key1, value1 in value0.items():
                    if type(value1) == list:
                        for item2 in value1:
                            if type(item2) == dict:
                                if 'key' in item2:
                                    line = [params['service'], param_group,
                                            str(key0), str(key1), 
                                            str(item2['key']),  
                                            str(item2['value'])]
                                    write_line(line, out_file)
                    else:
                        line = [params['service'], param_group,
                                str(key0), str(key1), str(value1)]
                        write_line(line, out_file)
            elif type(value0) == list:
                if all(isinstance(x, (int, bool, str)) for x in value0):
                    line = [params['service'], param_group, key0, str(value0)]
                    write_line(line, out_file)
                else:
                    for item1 in value0:
                        if type(item1) == dict:
                            if 'key' in item1:
                                line = [params['service'], param_group, 
                                        str(key0), str(item1['key']), 
                                        str(item1['value'])]
                                write_line(line, out_file)
                            else:
                                line = [params['service'], param_group, 
                                        str(key0), str(item1)]
                                write_line(line, out_file)
                        else:
                            line = [params['service'], param_group,
                                    str(key0), tr(item1)]
                            write_line(line, out_file)
            else:
                if key0 != 'name':
                    line = [params['service'], param_group, str(key0),
                            str(value0)]
                    write_line(line, out_file)

    out_file.close()
    print('wrote file ', params['out_file'])
    
if __name__ == '__main__': 
    main()
