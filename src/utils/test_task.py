from src.utils.copytotable import DPACopyToTable
from src.utils.tests import run_tests

class DPATestTask(DPACopyToTable):
    table = 'dpa.test_metadata'
    test_case = None
    columns = ['step', 'task_id', 'tests']
    test_result = None
    step = 0

    def run(self):
        if not self.test_case:
            raise Exception("Debes especificar un TestCase")

        data, metadata = self.input()

        self.test_case.data = data
        self.test_case.metadata = metadata

        self.test_result = run_tests(self.test_case)
        super().run()
    
    def rows(self):
        passed = self.test_result.testsRun
        
        yield (self.step, self.task_id, passed)

        