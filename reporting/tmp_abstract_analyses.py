


class SITL:
  def __init__(self, update_function):
    self.analyses = set()
    self.non_summary_analyses = []
    self.summary_analyses = []
    self.reports = set()
    self.analysis_edges = set()
    self.data = None
    self.all_maps = None
    self.update_function = update_function
    raise Exception("have not yet implemented data")


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
    prev_fold = None
    for i, datum in enumerate(data):
      current_analyses = dict()
      current_fold = dict()
      current_map = dict()
      self.update_function(datum)
      for analysis in self.non_summary_analyses:
        comp = analysis.compute(current_analyses, prev_fold)
        key = analysis.key
        if analysis.keep_as_map:
          current_map[key] = comp
        if analysis.keep_as_fold:
          current_fold[key] = comp
        current_analyses[analysis.key] = comp
      prev_fold = current_fold
      self.store_map(i, datum, current_map)
    current_analyses = self.all_da_maps
    for analysis in self.summary_analyses:
      current_analyses[analysis.key] = analysis.compute(current_analyses, prev_fold)
    reports = dict()
    for report in self.reports:
      report.report(current_analyses, prev_fold)

  def sort_analyses(self):
    nodes = self.analyses.keys()
    edges = self.analysis_edges
    s = nodes_without_incoming_edges(nodes, edges)
    topo_order = []
    while not is_empty(s):
      node_from = s.pop()
      topo_order += [node_from]
      nodes_to = [edge[1] in edges if edge[0] == node_from]
      for node_to in nodes_to:
        edges.remove((node_from, node_to))
        if not has_incoming_edges(edges, node_to):
          s.add(node_to)
    return topo_order

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


import reporters

class SwiftSITL():
  def __init__():
    self.reports = set([])
    self.all_analyses  = set([])
    self.map_analyses  = set([])
    self.fold_analyses = set([])
    self.ordered_online_analyses  = []
    self.ordered_summary_analyses = []
    raise Exception("still need to load data.")
    self.data = None

  def run():
    self.find_analyses_topological_order()
    analyze_data()

    raise Exception("Not implemented.")

  def analyze_data():
    previous_analyses = None
    for datum in self.data:
      analyses = dict()
      reports = dict()
      for analysis in self.ordered_online_analyses:
        analyses[analysis.label] = analysis.analyze(previous_analyses, analyses, datum)
      Exception("Then what do I do with the reports?")
      previous_analyses = analyses
    analyses = dict()
    reports = dict()
    for analysis in self.ordered_summary_analyses:
      analyses[analysis.label] = analyses.analyze(ALL_REPORTS_OR_ANALYSES)
    for report in summary_reports:
      reports[report.label] = report.report(analyses)

  def add_report(report):
    if not report in self.reports:
      self.reports.add(report)
      self.add_analysis(report)

  def add_analysis(analysis):
    if not analyis in self.analyses:
      self.analyses.add(analysis)
      for parent in analysis.parents:
        self.add_analysis(parent)

