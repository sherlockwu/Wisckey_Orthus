import sys
import re

report_file_name = sys.argv[1]

end_to_end_throughput = dict()

with open(report_file_name, 'r')  as report_file:
    for line in report_file:
        if 'miss' in line:
            continue
        # parse throughput vs time
        #if '... finished' in line:
        if '...' in line and 'finished' in line:
            #throughput_num =  line.split('ops,')[-1].replace('ops\/s', '')
            #throughput_timing =  int(re.sub("[^0-9]", "", line.split('ops,')[0]))
            #throughput_num =  float(re.sub("[^0-9.]", "", line.split('ops,')[-1]))
            line = line.split('ops,')[-1]
            throughput_timing =  int(re.sub("[^0-9]", "", line.split('ms,')[0]))
            throughput_num =  float(re.sub("[^0-9.]", "", line.split('ms,')[-1]))
            if (throughput_timing not in end_to_end_throughput):
                end_to_end_throughput[throughput_timing] = throughput_num
            else:
                end_to_end_throughput[throughput_timing] += throughput_num
            print throughput_timing, throughput_num

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
        if 'slope' in line:
            print line.strip('\n')
            #continue
        if 'ran for' in line:
            #continue
            for time in sorted(end_to_end_throughput):
                print time, end_to_end_throughput[time]
            end_to_end_throughput = dict()

end_to_end = ''
traffic = ''



