import argparse
import statistics
from multiprocessing import Pool
from time import sleep
from timeit import default_timer as timer


parser = argparse.ArgumentParser(description='Load test generator')
parser.add_argument('-s', type=int, default=4,      help='Server instances')
parser.add_argument('-c', type=int, default=16,     help='Client instances')
parser.add_argument('-e', type=int, default=1000,   help='Log entries count')
parser.add_argument('-d', type=int, default=100,    help=('Log entry dimention'
                    ' in bytes'))


def writer(entries_count, size=100, address='127.0.0.1', port=9110):
    d = DistributedDict(address, port)
    prefix = get_random_string(lenght=6)
    stub_value = 'a' * size
    for key in range(entries_count):
        d[prefix + str(key)] = stub_value
    return timer()


def reader(entries_count=100, address='127.0.0.1', port=9110):
    d = DistributedDict(address, port)
    keys = map(lambda x: x['key'], d.diagnostic['log']['data'])
    for key in keys:
        temp = d[key]
    return timer()


def client_pool(func, entries_count, workers, additional_args=[]):
    pool = Pool(workers)
    start_time = timer()
    worker_args = [[entries_count // workers] + additional_args]
    finish_times = pool.starmap(func, worker_args * workers)
    return (statistics.stdev(finish_times),
            statistics.mean(finish_times) - start_time)


if __name__ == '__main__':
    args = parser.parse_args()
    from utils import Pool as ServerPool
    from raft.client.distributedDict import DistributedDict
    from utils import get_random_string

    print('Starting {} server instances'.format(args.s))
    server_pool = ServerPool(args.s)
    server_pool.start(server_pool.ids)

    sleep(1)

    print('Setup completed')
    print('-' * 80)
    print('Started pushing data')
    stdev_time, elapsed_time = client_pool(writer, args.e, args.c, [args.d])
    print('Finished pushing {} key/value pairs of size {} kB '
          'with {} clients'.format(args.e, args.d / 1000, args.c))
    print('Duration: {:.2f} sec, stdev: {:.2}'.format(elapsed_time,
                                                      stdev_time))
    print('req/client: {}'.format(args.e // args.c))
    print('Cumulative req/s: {:.2f}'.format(args.e / elapsed_time))
    print('Average time/req (ms): {:.2f}'.format(elapsed_time / args.e * 1000))
    print('MB transferred: {:.3f}'.format(args.d * args.e / (10 ** 6)))
    print('-' * 80)
    print('Teardown')
    sleep(1)
    server_pool.stop(server_pool.ids)
    server_pool.rm(server_pool.ids)
