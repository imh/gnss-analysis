#test dat shit

from gnss_analysis.abstract_analysis.manage_tests import *
from gnss_analysis.abstract_analysis.analysis import *
from gnss_analysis.abstract_analysis.report import *


#A map that just holds the initial data
class DatumA(Analysis):
  def __init__(self):
    super(DatumA, self).__init__(
      key='data',
      keep_as_map=True)
  def compute(self, datum, current_analyses, prev_fold):
    return datum

#A fold to compute the sum of squares of the data points
class SumSquareA(Analysis):
  def __init__(self, keep_as_map=False):
    super(SumSquareA, self).__init__(
      key='sum_of_squares', 
      keep_as_fold=True,
      keep_as_map=keep_as_map,
      fold_init=0)
  def compute(self, datum, current_analyses, prev_fold):
    return prev_fold['sum_of_squares'] + datum**2  

#A fold to compute the sum of the data points
class SumA(Analysis):
  def __init__(self, keep_as_map=False):
    super(SumA, self).__init__(
      key='sum',
      keep_as_fold=True,
      keep_as_map=keep_as_map,
      fold_init=0)
  def compute(self, datum, current_analyses, prev_fold):
    return prev_fold['sum'] + datum

#A fold to count the data points
class CountA(Analysis):
  def __init__(self, keep_as_map=False):
    super(CountA, self).__init__(
      key='count',
      keep_as_fold=True,
      keep_as_map=keep_as_map,
      fold_init=0)
  def compute(self, datum, current_analyses, prev_fold):
    return prev_fold['count'] + 1

#A summary to compute the mean from folds
class MeanA(Analysis):
  def __init__(self):
    super(MeanA, self).__init__(
      key='mean',
      parents=set([SumA(), CountA()]),
      is_summary=True)
  def compute(self, data, current_analyses, final_fold):
    return float(final_fold['sum']) / float(final_fold['count'])

#A report for the data mean from folds
class MeanR(Report):
  def __init__(self):
    super(MeanR, self).__init__(
      key='mean',
      parents=set([MeanA()]))
  def report(self, data, analyses, folds):
    return str(analyses['mean'])

#A summary to compute the mean from maps
class MeanA2(Analysis):
  def __init__(self):
    super(MeanA2, self).__init__(
      key='mean2',
      parents=set([DatumA()]),
      is_summary=True)
  def compute(self, data, current_analyses, folds):
    print current_analyses['data'].mean()
    return current_analyses['data'].mean()

#A report for the data mean from maps
class MeanR2(Report):
  def __init__(self):
    super(MeanR2, self).__init__(
      key='mean2',
      parents=set([MeanA2()]))
  def report(self, data, analyses, folds):
    return str(analyses['mean2'])

#A summary to compute the variance of the data
class VarianceA(Analysis):
  def __init__(self):
    super(VarianceA, self).__init__(
      key='variance',
      parents=set([MeanA(), SumSquareA(), CountA()]),
      is_summary=True)
  def compute(self, data, current_analyses, final_fold):
    mean_of_squares = float(final_fold['sum_of_squares']) / float(final_fold['count'])
    square_of_mean = current_analyses['mean']**2
    return mean_of_squares - square_of_mean

#A report on the variance of the data
class VarianceR(Report):
  def __init__(self):
    super(VarianceR, self).__init__(key='variance',
      parents=set([VarianceA()]))
  def report(self, data, analyses, folds):
    return str(analyses['variance'])


if __name__ == '__main__':
  data = [1,2,3,4,5,6,7,8]
  update_function = lambda x: None
  tester = SITL(update_function, data)
  tester.add_report(MeanR())
  tester.add_report(VarianceR())
  tester.add_report(MeanR2())
  reports = tester.compute()
  for key, report in reports.iteritems():
    print '(key=' + key + ') \t' + str(report)
