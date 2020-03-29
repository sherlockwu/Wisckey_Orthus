import os
import sys


check_dir = sys.argv[1]


print('type threads readahead latency')

results = []


for subdir in os.listdir(check_dir):
    #print subdir
    res = ''
    res1 = ''
    res2 = ''
    res3 = ''
    jump = False
    for each_file in os.listdir(check_dir + '/' + subdir):
       if each_file == 'config.json':
           with open(check_dir + '/' + subdir + '/' + each_file, 'r')  as config_file:
               dict_from_file = eval(config_file.read())
               #res1 = dict_from_file['type'] + ' ' + str(dict_from_file['threads'])
               res1 = dict_from_file['type'] + ' ' + str(dict_from_file['threads']) + ' ' + str(dict_from_file['readahead'])
               if dict_from_file['threads'] != 4:
               #if 'write' not in dict_from_file['type']:
                   jump = False
       #if each_file == 'running':
       #    with open(check_dir + '/' + subdir + '/' + each_file, 'r')  as fio_output_file:
       #        lines = fio_output_file.readlines()
       #        res2 = lines[-5].split('Throughput:')[-1].strip('Tx\/s\n')

    if jump == True:
        continue
    print(res1+res2+res3) , subdir
