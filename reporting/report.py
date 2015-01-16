
class Report:
  def __init__(self, key, report_fun, parents):
    self.key = key
    self.report_fun = report_fun
    self.parents = parents

  def report(self, data, analyses, folds):
    return self.report_fun(data, analyses, folds)