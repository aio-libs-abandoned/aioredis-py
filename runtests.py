"""Run aioredis unittests.

Usage:
  python3 runtests.py [flags] [pattern] ...

Patterns are matched against the fully qualified name of the test,
including package, module, class and method,
e.g. 'tests.test_events.PolicyTests.testPolicy'.

For full help, try --help.

runtests.py --coverage is equivalent of:

  $(COVERAGE) run --branch runtests.py -v
  $(COVERAGE) html $(list of files)
  $(COVERAGE) report -m $(list of files)

"""

# Originally written by Beech Horn (for NDB).

import argparse
import gc
import logging
import os
import re
import shutil
import sys
import unittest
import traceback
import textwrap
import importlib.machinery
try:
    import coverage
except ImportError:
    coverage = None

from unittest.signals import installHandler

assert sys.version >= '3.3', 'Please use Python 3.3 or higher.'

ARGS = argparse.ArgumentParser(description="Run all unittests.")
ARGS.add_argument(
    '-v', action="store_true", dest='verbose',
    default=False, help='verbose')
ARGS.add_argument(
    '-x', action="store_true", dest='exclude', help='exclude tests')
ARGS.add_argument(
    '-f', '--failfast', action="store_true", default=False,
    dest='failfast', help='Stop on first fail or error')
ARGS.add_argument(
    '-c', '--catch', action="store_true", default=False,
    dest='catchbreak', help='Catch control-C and display results')
ARGS.add_argument(
    '--standalone-and-cluster', action="store_true", default=False,
    dest='standalone_and_cluster',
    help='Run all tests first against a standalone Redis, then against a '
    'cluster. Mainly for meaningful coverage reports.')
ARGS.add_argument(
    '--cluster', action="store_true", default=False,
    help='Run tests against a Redis cluster.')
ARGS.add_argument(
    '--forever', action="store_true", dest='forever', default=False,
    help='run tests forever to catch sporadic errors')
ARGS.add_argument(
    '--findleaks', action='store_true', dest='findleaks',
    help='detect tests that leak memory')
ARGS.add_argument(
    '--tests', action="store", dest='testsdir', default='tests',
    help='tests directory')
ARGS.add_argument(
    '--coverage', action="store_true", dest='coverage',
    help='enable html coverage report')
ARGS.add_argument(
    'pattern', action="store", nargs="*",
    help='optional regex patterns to match test ids (default all tests)')

COV_ARGS = argparse.ArgumentParser(description="Run all unittests.")
COV_ARGS.add_argument(
    '--coverage', action="store", dest='coverage', nargs='?', const='',
    help='enable coverage report and provide python files directory')


def load_modules(basedir, suffix='.py', *, verbose=False):
    def list_dir(prefix, dir):
        files = []

        modpath = os.path.join(dir, '__init__.py')
        if os.path.isfile(modpath):
            mod = os.path.split(dir)[-1]
            files.append(('{}{}'.format(prefix, mod), modpath))

            prefix = '{}{}.'.format(prefix, mod)

        for name in os.listdir(dir):
            path = os.path.join(dir, name)

            if os.path.isdir(path):
                files.extend(list_dir('{}{}.'.format(prefix, name), path))
            else:
                if (name != '__init__.py' and name.endswith(suffix) and
                        not name.startswith(('.', '_'))):
                    files.append(('{}{}'.format(prefix, name[:-3]), path))

        return files

    mods = []
    for modname, sourcefile in list_dir('', basedir):
        if modname == 'runtests':
            continue
        try:
            loader = importlib.machinery.SourceFileLoader(modname, sourcefile)
            mods.append((loader.load_module(), sourcefile))
        except SyntaxError:
            raise
        except Exception as err:
            print("Skipping '{}': {}".format(modname, err), file=sys.stderr)
            if verbose:
                try:
                    traceback.print_exc()
                except Exception:
                    pass
    return mods


class TestsFinder:

    def __init__(self, testsdir, includes=(), excludes=(), *,
                 verbose=False):
        self._testsdir = testsdir
        self._includes = includes
        self._excludes = excludes
        self._verbose = verbose
        self.find_available_tests()

    def find_available_tests(self):
        """
        Find available test classes without instantiating them.
        """
        self._test_factories = []
        mods = [mod for mod, _ in load_modules(self._testsdir,
                                               verbose=self._verbose)]
        for mod in mods:
            for name in set(dir(mod)):
                if name.endswith('Test'):
                    self._test_factories.append(getattr(mod, name))

    def _load_tests(self, running_on_cluster=False):
        """
        Load test cases from the available test classes and apply
        optional include / exclude filters.
        """
        loader = unittest.TestLoader()
        all_tests = []
        for test_factory in self._test_factories:
            tests = loader.loadTestsFromTestCase(test_factory)
            if self._includes:
                tests = [test
                         for test in tests
                         if any(re.search(pat, test.id())
                                for pat in self._includes)]
            if self._excludes:
                tests = [test
                         for test in tests
                         if not any(re.search(pat, test.id())
                                    for pat in self._excludes)]
            for test in tests:
                test._running_on_cluster = running_on_cluster

            all_tests.extend(tests)
        return all_tests

    def load_tests(self, standalone, cluster):
        all_tests = []
        if standalone:
            all_tests.extend(self._load_tests(False))
        if cluster:
            all_tests.extend(self._load_tests(True))
        return all_tests


class TestResult(unittest.TextTestResult):

    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.leaks = []

    def startTest(self, test):
        super().startTest(test)
        gc.collect()

    def addSuccess(self, test):
        super().addSuccess(test)
        gc.collect()
        if gc.garbage:
            if self.showAll:
                self.stream.writeln(
                    "    Warning: test created {} uncollectable "
                    "object(s).".format(len(gc.garbage)))
            # move the uncollectable objects somewhere so we don't see
            # them again
            self.leaks.append((self.getDescription(test), gc.garbage[:]))
            del gc.garbage[:]


class TestRunner(unittest.TextTestRunner):
    resultclass = TestResult

    def run(self, test):
        result = super().run(test)
        if result.leaks:
            self.stream.writeln("{} tests leaks:".format(len(result.leaks)))
            for name, leaks in result.leaks:
                self.stream.writeln(' '*4 + name + ':')
                for leak in leaks:
                    self.stream.writeln(' '*8 + repr(leak))
        return result


def runtests():
    args = ARGS.parse_args()

    if args.coverage and coverage is None:
        URL = "bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py"
        print(textwrap.dedent("""
            coverage package is not installed.

            To install coverage3 for Python 3, you need:
              - Setuptools (https://pypi.python.org/pypi/setuptools)

              What worked for me:
              - download {0}
                 * curl -O https://{0}
              - python3 ez_setup.py
              - python3 -m easy_install coverage
        """.format(URL)).strip())
        sys.exit(1)

    testsdir = os.path.abspath(args.testsdir)
    if not os.path.isdir(testsdir):
        print("Tests directory is not found: {}\n".format(testsdir))
        ARGS.print_help()
        return

    excludes = includes = []
    if args.exclude:
        excludes = args.pattern
    else:
        includes = args.pattern

    verbosity = args.verbose and 4 or 0
    failfast = args.failfast
    catchbreak = args.catchbreak
    findleaks = args.findleaks
    standalone = not args.cluster or args.standalone_and_cluster
    cluster = args.cluster or args.standalone_and_cluster
    runner_factory = TestRunner if findleaks else unittest.TextTestRunner

    if args.coverage:
        cov = coverage.coverage(branch=True,
                                source=['aioredis'],
                                # Used to set up the cluster under test.
                                omit=['aioredis/cluster/testcluster.py']
                                )
        cov.start()

    finder = TestsFinder(args.testsdir, includes, excludes,
                         verbose=args.verbose)
    logger = logging.getLogger('aioredis')
    logger.addHandler(logging.StreamHandler())
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.CRITICAL)
    if catchbreak:
        installHandler()

    def create_and_run_suite():
        suite = unittest.TestSuite()
        suite.addTests(finder.load_tests(standalone, cluster))
        return runner_factory(verbosity=verbosity, failfast=failfast)\
            .run(suite)

    try:
        if args.forever:
            while True:
                result = create_and_run_suite()
                if not result.wasSuccessful():
                    sys.exit(1)
        else:
            result = create_and_run_suite()
            sys.exit(not result.wasSuccessful())
    finally:
        if args.coverage:
            cov.stop()
            cov.save()
            if os.path.exists('htmlcov'):
                shutil.rmtree('htmlcov')
            cov.html_report(directory='htmlcov')
            print("\nCoverage report:")
            cov.report(show_missing=False)
            here = os.path.dirname(os.path.abspath(__file__))
            print("\nFor html report:")
            print("open file://{}/htmlcov/index.html".format(here))


if __name__ == '__main__':
    runtests()
