import luigi

class DPATask(luigi.Task):
    def input(self):
        return self.requires().requires().input()