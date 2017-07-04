# encoding: utf8
import argparse
import beanstalkc
from multiprocessing import Process, Pool
import time


def single(index):
    bmq = beanstalkc.Connection(host=args.host, port=args.port, parse_yaml=True)

    tube = 'test_%s' % index
    bmq.use(tube)
    for i in range(args.count):
        bmq.put('%s_%s' % (tube, i))


def main():
    pool = Pool(processes=args.procs)
    pool.map(single, range(args.procs))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('port', type=int, default=11133)
    parser.add_argument('--procs', '-p', type=int, default=1)
    parser.add_argument('--count', '-n', type=int, default=1000)
    args = parser.parse_args()

    main()
