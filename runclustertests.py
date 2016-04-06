import argparse
import os
import sys
from runtests import TestsFinder, TestRunner
from setupcluster import START_PORT

os.environ['REDIS_CLUSTER'] = 'true'
os.environ['REDIS_PORT'] = str(START_PORT)
if 'REDIS_VERSION' not in os.environ:
    os.environ['REDIS_VERSION'] = '3.0.0'

parser = argparse.ArgumentParser()
parser.add_argument(
    '-v', action="store_true", dest='verbose',
    default=False, help='verbose')
parser.add_argument(
    '-f', '--failfast', action="store_true", default=False,
    dest='failfast', help='Stop on first fail or error')
parser.add_argument(
    '--tests', action="store", dest='testsdir', default='tests',
    help='tests directory')
args = parser.parse_args()


verbosity = v = args.verbose and 4 or 0
finder = TestsFinder(args.testsdir, verbose=verbosity)

tests = finder.load_tests()
result = TestRunner(verbosity=args.verbose, failfast=args.failfast).run(tests)
sys.exit(not result.wasSuccessful())
