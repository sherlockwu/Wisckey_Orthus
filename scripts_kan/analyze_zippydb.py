import sys
import re

report_file_name = sys.argv[1]

end_to_end_throughput = dict()


def split(line):
    items = line.split(',')
    throughput_timing =  int(re.sub("[^0-9]", "", line.split('ms,')[0]))
    for i in range(0, len(items)):
        items[i] = int(re.sub("[^0-9]", "", items[i]))
    return items 

with open(report_file_name, 'r')  as report_file:
    for line in report_file:
        if 'miss ratio' in line:
            continue
        # parse throughput vs time
        #print line
        if '...' in line and 'finished' in line:
        #if 'finished' in line:
            #throughput_num =  line.split('ops,')[-1].replace('ops\/s', '')
            #throughput_timing =  int(re.sub("[^0-9]", "", line.split('ops,')[0]))
            #throughput_num =  float(re.sub("[^0-9.]", "", line.split('ops,')[-1]))
            items = split(line)
            print items[0], items[2], items[3]/10.0, items[7], items[8] # #threads, timing, throughput_num, data admission ratio, load admission ratio

        if 'Data admit ratio' in line:
            data_admit_ratio =  int(re.sub("[^0-9]", "", line.split('Load')[0]))
            load_admit_ratio =  int(re.sub("[^0-9.]", "", line.split('Load')[-1]))
            #print data_admit_ratio, load_admit_ratio
        
        if 'Optane read throughput' in line:
            line = re.sub("-nan", "0", line)
            optane_read_throughput =  float(re.sub("[^0-9.]", "", line.split(';')[0]))
            optane_write_throughput =  float(re.sub("[^0-9.]", "", line.split(';')[1]))
            flash_read_throughput =  float(re.sub("[^0-9.]", "", line.split(';')[2]))
            overall_read_throughput =  float(re.sub("[^0-9.]", "", line.split(';')[3]))
            #print optane_read_throughput, optane_write_throughput, flash_read_throughput, overall_read_throughput
        if 'ran for' in line:
            #continue
            for time in sorted(end_to_end_throughput):
                print time, end_to_end_throughput[time]
            end_to_end_throughput = dict()

end_to_end = ''
traffic = ''



