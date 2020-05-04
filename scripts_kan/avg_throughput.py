import sys
import re

report_file_name = sys.argv[1]

end_to_end_throughput = dict()

with open(report_file_name, 'r')  as report_file:
    counter = 0;
    optane_read_throughput = 0.0
    optane_write_throughput = 0.0;
    flash_read_throughput = 0.0;
    overall_read_throughput = 0.0;

    for line in report_file:
        counter += 1
        line = re.sub("-nan", "0", line)
        items = line.split(' ')
        optane_read_throughput +=  float(items[0])
        optane_write_throughput +=  float(items[1])
        flash_read_throughput +=  float(items[2])
        overall_read_throughput +=  float(items[3])

        if (counter % 10 == 0): 
            print 5*counter, optane_read_throughput/10, optane_write_throughput/10, flash_read_throughput/10, overall_read_throughput/10
            optane_read_throughput = 0.0
            optane_write_throughput = 0.0;
            flash_read_throughput = 0.0;
            overall_read_throughput = 0.0;

end_to_end = ''
traffic = ''



