from kedro.io.core import AbstractDataSet
import os
import pandas as pd
from kedro.extras.datasets.api import APIDataSet
from kedro.extras.datasets.pandas import CSVDataSet
from rebase.util import merge


class DynamicProvider(APIDataSet):

  base_url = 'https://dev-api.rebase.energy'

  def __init__(self, name, meta={}, features=[], start_date=None, end_date=None, options={}):
    super().__init__(
        url=f"{self.base_url}/weather/v2/query",
        params={
          'model': name,
          'start-date': start_date,
          'end-date': end_date,
          'latitude': meta['lat'],
          'longitude': meta['lon'],
          'variables': ','.join(features),
          'output-format': 'json',
          **options,
      },
      headers={
        'Authorization': os.environ.get('RB_API_KEY')
     }
    )

  def load(self):
    resp = super().load()
    data = resp.json()
    df = pd.DataFrame(data=data[0])
    df.index = pd.MultiIndex.from_arrays(
             [pd.to_datetime(df['ref_datetime'].values),
             pd.to_datetime(df['valid_datetime'].values)],
             names=['ref_datetime', 'valid_datetime'])
    # Drop now duplicated index columns
    df = df.drop(columns=['ref_datetime', 'valid_datetime'])
    return df


class LocalProvider(CSVDataSet):

    def _load(self) -> pd.DataFrame:
        df = super()._load()
        df['valid_datetime'] = pd.to_datetime(df['valid_datetime'])
        df = df.set_index('valid_datetime')
        return df

class Dataset(AbstractDataSet):

    name = None

    provider = None

    def __init__(
        self, 
        name, 
        target=None, 
        meta=None, 
        start_date=None,
        end_date=None,
        indexes={},
        features=[],
        options={}
    ):
        self.name = name
        self.indexes = indexes
        self.target = target

        # basic way to check if it's a file
        if '.' in name:
            self.provider = LocalProvider(name)
        else:
            self.provider = DynamicProvider(name, meta, features, start_date, end_date, options)


    def _load(self):
        return self.provider.load()


    def _describe(self):
        pass

    def _save(self, value):
        return self.provider.save(value)

    def merge(self, ds2, method):
        return MergedDataset(self.provider, ds2, method)


class MergedDataset(AbstractDataSet):

    datasets = [] 

    def __init__(self, ds1, ds2, method) -> None:
        self.datasets = [ds1, ds2]
        self.method = method

    def _load(self):
        return merge(
            self.datasets[0].load(), 
            self.datasets[1].load(),
            self.method
        )

    def _describe(self):
        pass

    def _save(self, value):
        pass
        #return self.provider.save(value)
