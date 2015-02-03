
import pandas as pd

class SITL:
  def __init__(self, update_function, data):
    self.analyses = dict()
    self.non_summary_analyses = []
    self.summary_analyses = []
    self.reports = set()
    self.analysis_edges = set()
    self.data = data
    self.all_maps = None
    self.update_function = update_function


  def add_report(self, report):
    self.reports.add(report)
    for analysis in report.parents:
      self.add_analysis(analysis)

  def add_analysis(self, analysis):
    if analysis.key in self.analyses.keys():
      self.analyses[analysis.key].merge_storage(analysis)
    else:
      self.analyses[analysis.key] = analysis
    for parent in analysis.parents:
      self.add_analysis(parent)
      self.analysis_edges.add( (parent.key, analysis.key) )

  def compute(self):
    self.sort_analyses()
    prev_fold = fold_inits(self.non_summary_analyses)
    maps = dict()

    for i, datum in enumerate(self.data):
      #initialize
      current_analyses = dict()
      current_fold = dict()
      current_map = dict()

      #update
      self.update_function(datum)

      # compute everything and put it away for other computations
      for analysis in self.non_summary_analyses:
        comp = analysis.compute(datum, current_analyses, prev_fold)
        key = analysis.key
        if analysis.keep_as_map:
          current_map[key] = comp
        if analysis.keep_as_fold:
          current_fold[key] = comp
        current_analyses[analysis.key] = comp

      #store analyses for later
      prev_fold = current_fold
      maps[i] = current_map
    analyses = pandafy(maps)
    analyses = maps #TODO analyses need to return columns
    for analysis in self.summary_analyses:
      analyses[analysis.key] = analysis.compute(self.data, analyses, prev_fold)
    reports = dict()
    for report in self.reports:
      reports[report.key] = report.report(self.data, analyses, prev_fold)
    return reports

  def sort_analyses(self):
    nodes = self.analyses.keys()
    edges = self.analysis_edges
    s = nodes_without_incoming_edges(nodes, edges)
    self.non_summary_analyses = []
    self.summary_analyses = []
    while not is_empty(s):
      node_from = s.pop()
      analysis = self.analyses[node_from]
      if analysis.is_summary:
        self.summary_analyses.append(analysis)
      else:
        self.non_summary_analyses.append(analysis)
      nodes_to = [edge[1] for edge in edges if edge[0] == node_from]
      for node_to in nodes_to:
        edges.remove((node_from, node_to))
        if not has_incoming_edges(edges, node_to):
          s.append(node_to)

def pandafy(maps):
  df = pd.DataFrame(maps).T
  d = dict()
  for key in df.keys():
    d[key] = df[key]
  return d


def fold_inits(non_summary_analyses):
  folds = dict()
  for analysis in non_summary_analyses:
    if analysis.keep_as_fold:
      folds[analysis.key] = analysis.fold_init
  return folds

def nodes_without_incoming_edges(nodes, edges):
  #NOTE can't just go through the edge list because some nodes have no edges
  return [node for node in nodes if not has_incoming_edges(edges, node)]

def has_incoming_edges(edges, node):
  for edge in edges:
    if edge[1] == node:
      return True
  return False

def is_empty(has_len):
  return len(has_len) == 0

