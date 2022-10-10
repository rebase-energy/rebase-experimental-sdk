import importlib
import kedro
from kedro.io import DataCatalog
from kedro.io.core import AbstractDataSet
from kedro.io import MemoryDataSet
from kedro.extras.datasets.pickle import PickleDataSet
from kedro.runner.sequential_runner import SequentialRunner
from rebase.util import install_repo


# class Pipeline():

#   def __init__(self, nodes, data_catalog) -> None:
#       kedro_nodes = []
#       for n in nodes:
#         func, inputs, outputs = n(_internal_inspect=True)

#         k_node = kedro.pipeline.node(
#             func, 
#             inputs=inputs,
#             outputs=outputs
#         )
#         kedro_nodes.append(k_node)
#       self.kedro_pipeline = (kedro.pipeline.pipeline(kedro_nodes))
#       self.data_catalog = data_catalog

#   def run(self):
#     runner = SequentialRunner()
#     return runner.run(self.kedro_pipeline, self.data_catalog)

def make_data_catalog(data):
  for key in data:
    if not isinstance(data[key], AbstractDataSet):
      #data[key] = MemoryDataSet(data[key])
      value = data[key]
      data[key] = PickleDataSet(filepath=f"./cache/{key}")
      data[key].save(value)
  return DataCatalog(data)
    

  

def pipeline(nodes, mc):
  kedro_nodes = []
  for n in nodes:
    func, inputs, outputs = n(_internal_inspect=True)

    k_node = kedro.pipeline.node(
        func, 
        inputs=inputs,
        outputs=outputs
    )
    kedro_nodes.append(k_node)
  kedro_pipeline = (kedro.pipeline.pipeline(kedro_nodes))

  def run(data):
    runner = SequentialRunner()
    data_catalog = make_data_catalog(data)
    result = runner.run(kedro_pipeline, data_catalog)
    return result

  return run

class ModelChain():

  installed = False

  repo_name = None

  def __init__(self, pipelines_or_repo):
    #self.data_catalog = DataCatalog(data)
    if type(pipelines_or_repo) is str:
      self.repo_name = pipelines_or_repo
    else:
      self._init_pipelines(pipelines_or_repo)

  @classmethod
  def _import(cls, module_name="src"):
      module = importlib.import_module(module_name)
      return getattr(module, 'init')

  @classmethod
  def from_local_module(cls):
      func = ModelChain._import()
      return ModelChain(func())


  def install(self):
      install_repo(self.repo_name)
      func = ModelChain._import()
      self._init_pipelines(func())

  
  def _init_pipelines(self, pipelines):
      for key in pipelines:
        setattr(self, key, pipeline(pipelines[key], self))
      
      self.installed = True
      

