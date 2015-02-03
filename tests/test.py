#test dat shit

from tmp_abstract_analyses import *
from report import *
from analysis import *

#A fold to compute the sum of squares
def sum_square_c(datum, current_analyses, prev_fold):
  return prev_fold['sum_of_squares'] + datum**2

def sum_square_a(keep_as_map=False):
  return Analysis('sum_of_squares',
                  sum_square_c,
                  keep_as_fold=True,
                  keep_as_map=keep_as_map,
                  fold_init=0)


#A fold to compute the sum
def sum_c(datum, current_analyses, prev_fold):
  return prev_fold['sum'] + datum

def sum_a(keep_as_map=False):
  return Analysis('sum',
                  sum_c,
                  keep_as_fold=True,
                  keep_as_map=keep_as_map,
                  fold_init=0)


#A fold to compute the count
def count_c(none, current_analyses, prev_fold):
  return prev_fold['count'] + 1

def count_a(keep_as_map=False):
  return Analysis('count',
                  count_c,
                  keep_as_fold=True,
                  keep_as_map=keep_as_map,
                  fold_init=0)

#A summary and report to compute the mean
def mean_c(none, current_analyses, final_fold):
  return float(final_fold['sum']) / float(final_fold['count'])

def mean_a():
  return Analysis('mean', 
                  mean_c,
                  parents=set([sum_a(), count_a()]),
                  is_summary=True)

def mean_rc(none, analyses, folds):
  return 'mean: ' + str(analyses['mean'])

def mean_report():
  return Report('mean',
                mean_rc,
                parents=set([mean_a()]))


#A summary and report to compute the variance
def variance_c(none, current_analyses, final_fold):
  mean_of_squares = float(final_fold['sum_of_squares']) / float(final_fold['count'])
  square_of_mean = current_analyses['mean']**2
  return mean_of_squares - square_of_mean

def variance_a():
  return Analysis('variance',
                  variance_c,
                  parents=set([mean_a(), sum_square_a(), count_a()]),
                  is_summary=True)

def variance_rc(none, analyses, folds):
  return 'variance: ' + str(analyses['variance'])

def variance_report(): 
  return Report('variance',
                variance_rc,
                parents=set([variance_a()]))


if __name__ == '__main__':
  data = [1,2,3,4,5,6,7,8]
  update_function = lambda x: None
  tester = SITL(update_function, data)
  tester.add_report(mean_report())
  tester.add_report(variance_report())
  reports = tester.compute()
  for report in reports.iteritems():
    print '(key=' + report[0] + ') \t' + str(report[1])
