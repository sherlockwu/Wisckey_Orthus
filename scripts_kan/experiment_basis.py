import itertools
from pyreuse.helpers import *
from pyreuse.sysutils.cgroup import Cgroup
import os.path
import time
import json
import os
import datetime

from pyreuse.sysutils.blocktrace import *
from pyreuse.sysutils.ncq import *
from pyreuse.sysutils.iostat_parser import *

# basis
KB = 1024
MB = 1024 * KB
GB = 1024 * MB 


# experiment setup
class Experiment(object):
    def __init__(self):
        # config something
        self.exp_name = 'leveldb/testing'
        self.home_dir = '/home/kanwu/Research/'
        self.res_dir = self.home_dir + 'results/' + self.exp_name
        self.tmp_dir = '/dev/shm/'
        self.dev_dir = '/dev/nvme1n1'
        prepare_dir(self.res_dir)
       
        # tools config
        self.tools_config = {
            'clear_page_cache': True,   # whether clear page cache before each run 
            'blktrace'        : False,   # check block IOs
            'iostat'          : False,  # check ios and cpu/io utilization
            'perf'            : False,  # draw flamegraph
            'sar'             : False   # check page faults
        }

        # experiment config
        config = {
          'type': ['randomread'],
          #'threads': [1, 2, 4, 8, 16, 32],
          'threads': [16],
          'memory': [1*GB],    #'memory limit'
          'swapiness': [0],
          #'readahead': [128, 0, 16]    # default 128KB
          'readahead': [16]    # default 128KB
        }

        # handle
        self.handle_config(config) 
        print '========================= overall ', len(self.all_configs), ' experiments ============================='
        print '==================== results in ', self.res_dir, ' ============================'
 
    def handle_config(self, config):
        config_dic = list(config.keys())
        config_lists = list(config.values())

        self.all_configs = []
        for element in itertools.product(*config_lists):
            new_config = dict(list(itertools.izip(config_dic, list(element))))
            self.all_configs.append(new_config)
        
    def dump_config(self, config):
        self.cur_exp_dir = self.res_dir + '/' + datetime.now().strftime("%H-%M-%S_%Y%m%d")
        os.mkdir(self.cur_exp_dir)
        with open(self.cur_exp_dir + '/config.json', 'w') as config_output:
            json.dump(config, config_output)

    def before_each(self, config):
        print '                ********* Configured with **********'
        print config
        self.dump_config(config)
        
        # set readahead
        shcmd('echo ' + str(config['readahead']) + ' > /sys/class/block/nvme0n1/queue/read_ahead_kb')
        shcmd('echo ' + str(config['readahead']) + ' > /sys/class/block/nvme1n1/queue/read_ahead_kb')
        
        # set cgroup
        self.cg = Cgroup(name='charlie', subs='memory')
        self.cg.set_item('memory', 'memory.limit_in_bytes', config['memory'])
        self.cg.set_item('memory', 'memory.swappiness', config['swapiness'])
        
        # clear page cache
        if self.tools_config['clear_page_cache']:
            shcmd('sync; echo 3 > /proc/sys/vm/drop_caches; sleep 1')
        # start iostat
        if self.tools_config['iostat']:
            shcmd('pkill iostat', ignore_error = True)
            shcmd('iostat -mx 1 /dev/nvme0n1 /dev/nvme1n1  > ' + self.tmp_dir + '/iostat.out &')
            #shcmd('iostat -mx 1 /dev/nvme0n1p3 > ' + self.cur_exp_dir + '/iostat.out &')   # to identify swapping

        # start blktrace
        stop_blktrace_on_bg()
        if self.tools_config['blktrace']:
            start_blktrace_on_bg(self.dev_dir, self.tmp_dir +'/blktrace.output', ['issue', 'complete'])
            time.sleep(2)
       
 
    def exp(self, config):
        print '              *********** start running ***********'
        #cmd = '/home/kanwu/Research/739-wisckey/db_bench --db=/mnt/970/db_64 --value_size=64 --cache_size=0 --compression_ratio=1 --benchmarks=readrandom --use_existing_db=1 --db_num=1342177280 --reads=100000 ' + '--threads=' + str(config['threads']) #+ ' > /dev/shm/running'
        cmd = '/home/kanwu/Research/739-wisckey/db_bench --db=/mnt/970/db_64 --value_size=64 --cache_size=0 --compression_ratio=1 --benchmarks=readrandom --use_existing_db=1 --db_num=100000000 --reads=500000 ' + '--threads=' + str(config['threads']) #+ ' > /dev/shm/running'
        #cmd = '/home/kanwu/Research/739-wisckey/db_bench --db=/mnt/optane/db_64 --value_size=64 --cache_size=0 --compression_ratio=1 --benchmarks=readrandom --use_existing_db=1 --db_num=1342177280 --reads=200000 ' + '--threads=' + str(config['threads']) #+ ' > /dev/shm/running'
        
        #shcmd(cmd)
        print cmd
        p = self.cg.execute(shlex.split(cmd))
        p.wait()

    def handle_iostat_out(self, iostat_output):
        print "==== utilization statistics ===="
        stats = parse_batch(iostat_output.read())
        with open(self.cur_exp_dir + '/iostat.out.cpu_parsed', 'w') as parsed_iostat:
            parsed_iostat.write('iowait system user idle \n')
            item_len = average_iowait = average_system = average_user = average_idle = 0
            for item in stats['cpu']:
                parsed_iostat.write(str(item['iowait']) + ' ' + str(item['system']) + ' ' + str(item['user']) + ' ' + str(item['idle']) + '\n')
                if float(item['idle']) > 79:
                    continue
                item_len += 1
                average_iowait += float(item['iowait'])
                average_system += float(item['system'])
                average_user += float(item['user'])
                average_idle += float(item['idle'])
            if item_len > 0:
                print 'iowait  system  user  idle'
                print str(average_iowait/item_len), str(average_system/item_len), str(average_user/item_len), str(average_idle/item_len)
            else:
                print 'seems too idle of CPU'

        with open(self.cur_exp_dir + '/iostat.out.disk_parsed', 'w') as parsed_iostat:
            parsed_iostat.write('r_iops r_bw(MB/s) w_iops w_bw(MB/s) avgrq_sz(KB) avgqu_sz\n')
            item_len = average_rbw = average_wbw = 0
            for item in stats['io']:
                #parsed_iostat.write(item['r/s'] + ' ' + item['rMB/s'] + ' ' + item['w/s'] + ' ' + item['wMB/s'] + ' ' + str(float(item['avgrq-sz'])*512/1024) + ' '+ item['avgqu-sz'] +'\n')
                parsed_iostat.write(item['r/s'] + ' ' + item['rMB/s'] + ' ' + item['w/s'] + ' ' + item['wMB/s'] + ' ' + str(float(item['rareq-sz'])*512/1024) + ' '+ item['aqu-sz'] +'\n')
                if float(item['rMB/s']) + float(item['wMB/s']) < 20:
                    continue
                item_len += 1
                average_rbw += float(item['rMB/s'])
                average_wbw += float(item['wMB/s'])
            if item_len > 0:
                print str(average_rbw/item_len), str(average_wbw/item_len)
            else:
                print 'seems too idle of Disk'
        print "================================="    

    def latency_stat(self, events):
        min_latency = 100000.0
        avg_latency = 0.0
        count = 0
        max_latency = 0.0
        lats = []
        for line in events:
            lat = float(line.split()[-2])
            if lat > 0:
                lats.append(lat)
                count += 1
                avg_latency += lat
                if lat < min_latency:
                    min_latency = lat
                if lat > max_latency:
                    max_latency = lat
        with open(self.cur_exp_dir + '/latency.notsorted','w') as lat_out:
            for lat in lats:
                lat_out.write(str(lat * 1000000) +'\n')
        with open(self.cur_exp_dir + '/latency.sort','w') as lat_out:
            for lat in sorted(lats):
                lat_out.write(str(lat * 1000000) +'\n')

        with open(self.cur_exp_dir + '/latency.stat','w') as lat_out:
            lat_out.write('======= Latency statistics ========\n')
            lat_out.write('  num_req: ' +  str(count) + '\n')
            lat_out.write('  average: ' + str(avg_latency/count * 1000000) + 'us\n')
            lat_out.write('  min_lat: ' + str(min_latency * 1000000) + 'us\n')
            lat_out.write('  max_lat: ' + str(max_latency * 1000000) + 'us\n')
            lat_out.write('===================================')
    
    def after_each(self, config):
        print '              **************** done ***************'


        if self.tools_config['iostat'] or self.tools_config['sar'] or self.tools_config['blktrace']:
            shcmd("sleep 1; sync; sleep 1")
        # kill something at first, not to introduce IOs during analysis 
        if self.tools_config['iostat']:
            shcmd('pkill iostat; sleep 1')
        if self.tools_config['sar']:
            shcmd('pkill sar')
        if self.tools_config['blktrace']:
            stop_blktrace_on_bg()
        
        if self.tools_config['iostat'] or self.tools_config['sar'] or self.tools_config['blktrace']:
            shcmd("sync; sleep 1")

        # copy running results(contain throughput)
        shcmd('cp ' + self.tmp_dir + '/running ' + self.cur_exp_dir + '/running', ignore_error=True)

        # wrapup iostat
        if self.tools_config['iostat']:
            shcmd('cp ' + self.tmp_dir + '/iostat.out ' + self.cur_exp_dir + '/iostat.out')
            with open(self.tmp_dir + '/iostat.out') as iostat_output:
                self.handle_iostat_out(iostat_output)
        # wrapup sar
        if self.tools_config['sar']:
            shcmd('cp ' + self.tmp_dir + '/sar.out ' + self.cur_exp_dir + '/sar.out')

        # wrapup flamegraph(perf)
        if self.tools_config['perf']:
            shcmd('cp perf.data ' + self.cur_exp_dir + '/')
            #shcmd('perf script | /mnt/ssd/fio_test/experiments/FlameGraph/stackcollapse-perf.pl > ' + self.cur_exp_dir + '/out.perf-folded')
            #shcmd('perf script | /mnt/ssd/fio_test/experiments/FlameGraph/stackcollapse-perf.pl > out.perf-folded')
            #shcmd('/mnt/ssd/fio_test/experiments/FlameGraph/flamegraph.pl out.perf-folded > ' + self.cur_exp_dir + '/perf-kernel.svg')
        
        # wrapup blktrace
        if self.tools_config['blktrace']:
            shcmd('cp ' + self.tmp_dir + '/blktrace.output ' + self.cur_exp_dir + '/blktrace.output')
            blkresult = BlktraceResultInMem(
                    sector_size=512,
                    event_file_column_names=['pid', 'action', 'operation', 'offset', 'size',
                        'timestamp', 'pre_wait_time', 'latency', 'sync'],
                    raw_blkparse_file_path=self.tmp_dir+'/blktrace.output',
                    parsed_output_path=self.cur_exp_dir+'/blkparse-output.txt.parsed')
            
            blkresult.create_event_file()
            with open(self.cur_exp_dir + '/blkparse-output.txt.parsed','r') as event_file:
                self.latency_stat(event_file)

            return
            # generate ncq
            table = parse_ncq(event_path = self.cur_exp_dir + '/blkparse-output.txt.parsed')
            with open(self.cur_exp_dir + '/ncq.txt','w') as ncq_output:
                for item in table:
                    ncq_output.write("%s\n" % ' '.join(str(e) for e in [item['pid'], item['action'], item['operation'], item['offset'], item['size'], item['timestamp'], item['pre_depth'], item['post_depth']]))

    def run(self):
        for config in self.all_configs:
            self.before_each(config)
            self.exp(config)
            self.after_each(config)

if __name__=='__main__':

    exp = Experiment()
    exp.run()
