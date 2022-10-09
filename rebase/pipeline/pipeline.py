import kedro
from kedro.io import DataCatalog
from kedro.io.core import AbstractDataSet
from kedro.io import MemoryDataSet
from kedro.runner.sequential_runner import SequentialRunner


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
      data[key] = MemoryDataSet(data[key])
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


  def __init__(self, pipelines):
    #self.data_catalog = DataCatalog(data)

    for key in pipelines:
      setattr(self, key, pipeline(pipelines[key], self))
      

