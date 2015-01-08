
import inspect

class Analysis:
  def __init__(self, key, computation, parents, keep_as_map=False, keep_as_fold=False, is_summary=False):
    self.parents = parents
    self.key = key
    self.keep_as_map = keep_as_map
    self.keep_as_fold = keep_as_fold
    self.is_summary = is_summary
    self.computation = computation
    self.check_valid()

  def merge_storage(self, other):
    self.keep_as_map = self.keep_as_map or other.keep_as_map
    self.keep_as_fold = self.keep_as_fold or other.keep_as_fold
    self.keep_as_summary = self.keep_as_summary or other.keep_as_summary

  def compute(self, analyses):
    return self.computation(analyses)

  def check_valid(self):
    #make sure that it has good properties
    valid = True
    # must be kept as map, fold, or summary
    valid = valid and (self.keep_as_map or self.keep_as_fold or self.is_summary)
    # cannot be map, fold, and summary
    valid = valid and not (self.is_summary and (self.keep_as_map or self.keep_as_fold))
    # TODO make sure there are no circular dependencies 
    #      (as of current code structure, shouldn't be possible to make one)