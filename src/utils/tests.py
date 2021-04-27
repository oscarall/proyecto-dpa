
import marbles.core
import unittest
import sys

def run_tests(test_case: marbles.core.TestCase):
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case)
    test = unittest.TextTestRunner().run(suite)

    if not test.wasSuccessful():
        raise Exception(test.failures[0][1])
    
    return test