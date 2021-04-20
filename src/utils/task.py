import luigi

class DPATask(luigi.Task):
    def input(self):
        return luigi.task.getpaths(self.requires()) if isinstance(self.requires(), (list, tuple)) else self.requires().input() 