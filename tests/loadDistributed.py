import argparse
import json
import os
import shutil
import asyncio
import datetime
import dateutil.parser
from time import sleep
from multiprocessing import Process, Pool
from utils import get_random_string
from raft.server.main import setup as serverSetup
from raft.client.distributedDict import DistributedDict


class Server:
    def __init__(self, args):
        self.address = (args.address, args.port)
        self.config = {'address': self.address, 'debug': False,
                       'cluster': {self.address},
                       'storage': 'raft.{}.persist'.format(args.port)}
        self.leader_address = (args.leader_address, args.leader_port)
        self.create_server()

        while True:
            sleep(2)
            self.repeat()

    def repeat(self):
        self.leader.refresh()
        if 'restart' not in self.leader:
            return
        sleep(2)  # for other nodes to acknowledge restart
        print('Stopping server')
        self.server.terminate()
        sleep(2)
        print('Removing files')
        shutil.rmtree('raft.{}.persist'.format(self.address[1]))
        print('Starting')
        self.create_server()

    def create_server(self, is_leader=True):
        def server_factory(config):
            serverSetup(config)
            loop = asyncio.get_event_loop()
            loop.run_forever()

        self.server = Process(target=server_factory, args=(self.config,))
        self.server.start()

        if is_leader and self.address == self.leader_address:
            sleep(1)
            print('Restarting Leader to increment term')
            self.server.terminate()
            self.create_server(is_leader=False)  # prevents recurtion
            sleep(1)
        else:
            sleep(3)
        self.leader = DistributedDict(*self.leader_address)
        self.leader.config_cluster('add', *self.address)


class Client:
    def __init__(self, args):
        self.prefix = get_random_string(lenght=6)
        self.last_job = None
        self.leader_address = (args.leader_address, args.leader_port)
        self.leader = DistributedDict(*self.leader_address)

        while True:
            try:
                self.repeat()
            except ConnectionRefusedError:
                print('Connection refused')
            except KeyError:
                print('Server Initialization')
            sleep(7 if self.last_job else 1)

    def broadcast(self, term, read=False):
        handle = '{}_{}'.format(term, self.prefix)
        self.leader = DistributedDict(*self.leader_address)
        if read:
            return handle in self.leader
        else:
            self.leader[handle] = True

    def repeat(self):
        self.leader.refresh()
        if not self.broadcast('checkin', read=True):
            self.broadcast('checkin')
            self.last_job = None
            print('Restarted')
        if 'job' not in self.leader:
            return
        job = self.leader['job']
        start_time = dateutil.parser.parse(job['start_time'])
        if start_time == self.last_job:
            return
        if start_time > datetime.datetime.now():
            return
        self.last_job = start_time
        self.run_pool(
            self.worker_writer, job['workers'],
            [job['entries'] // job['clients'], job['dimention'],
             self.leader_address[0], self.leader_address[1]])

    def run_pool(self, func, workers, arguments=[]):
            print('Running', workers)
            pool = Pool(workers)
            arguments[0] = arguments[0] // workers
            pool.starmap(func, [arguments] * workers)
            pool.terminate()
            print('Terminated')

    def worker_writer(self, entries, dimention, address, port):
        client = DistributedDict(address, port)
        worker_prefix = get_random_string(lenght=6)
        stub_value = 'a' * dimention
        for key in range(entries):
            client[worker_prefix + str(key)] = stub_value


class Head:
    def __init__(self, args):
        self.leader_address = (args.leader_address, args.leader_port)
        self.leader = DistributedDict(*self.leader_address)
        self.results = []

        print('Found {} test cases'.format(len(test_cases)))
        for n, test_case in enumerate(test_cases):
            ok = False
            while not ok:
                try:
                    print('Executing test {} of {}'.format(n, len(test_cases)))
                    self.send_test(test_case)
                except Exception as e:
                    print('Error!')
                    print(e)
                ok = input('Satisfied with result? ') == 'y'
                comment = input('Comment?')
                if ok:
                    self.results[-1]['comment'] = comment
                    with open('result_{}.json'.format(
                            datetime.datetime.now().isoformat()), 'w+') as f:
                        f.write(json.dumps(self.results, indent=2,
                                           sort_keys=True))
                else:
                    del self.results[-1]

    def send_test(self, case):
        print('Restarting...')
        self.leader['restart'] = True
        ready = False
        sleep(7)
        while not ready:
            try:
                self.leader.refresh()
                client_count = len(list(filter(
                    lambda x: x.startswith('checkin'), self.leader)))
                print('Servers {}, Clients {}'.format(
                    len(self.leader['cluster']), client_count))
                ready = len(self.leader['cluster']) == expected_servers and\
                    client_count == expected_clients
                if ready:
                    ready = input('Ready? ') == 'y'
                else:
                    sleep(1)

            except Exception as e:
                print('Errors while waiting for clients to checkin', e)
                sleep(1)

        start_time = datetime.datetime.now() + datetime.timedelta(seconds=3)
        case.update({'clients': client_count,
                     'start_time': start_time.isoformat()})
        self.leader['job'] = case
        print('Executing', case)
        self.collect_stats(start_time, case)

    def collect_stats(self, start_time, case):
        count = 0
        while self.leader.diagnostic['log']['commitIndex'] <= case['entries']:
            sleep(0.2)
            count += 1
            if count == 5 * 30:  # 30 seconds
                count = 0
                if input('Break? '):
                    break

        finish_time = datetime.datetime.now()
        print('Done writing in ', finish_time - start_time)
        self.results.append(
            {'elapsed_time': (finish_time - start_time).total_seconds(),
             'config': case,
             'diagnostic': self.leader.diagnostic})


# test_cases = [{'entries': 5000, 'workers': 128, 'dimention': 100}]
test_cases = [{'entries': entries, 'workers': workers, 'dimention': dimention}
              for entries in [10, 100, 1000, 5000, 10000]
              for workers in [10, 100, 256]
              for dimention in [100, 1000]
              if entries > workers]
expected_servers = 2
expected_clients = 1


types = {'server': Server, 'client': Client, 'head': Head}

parser = argparse.ArgumentParser(description='Load test client')
parser.add_argument('--type', choices=types.keys(), help='Type of instance')
parser.add_argument('--address', help='Server IP')
parser.add_argument('--port', type=int, help='Server Port')
parser.add_argument('--leader-address', help='leader Server IP')
parser.add_argument('--leader-port', type=int, help='leader Server Port')

if __name__ == '__main__':
    args = parser.parse_args()
    types[args.type](args)
