from rebase import Dataset, ModelChain, node
import pandas as pd
import lightgbm as lgb


@node(inputs=['train_set', 'params'], outputs=['model', 'metrics'])
def train(train_set, params):
    print(train_set)
    df_X = train_set.drop(columns=['target'])
    df_y = train_set['target']
    lgb_trainset = lgb.Dataset(df_X, label=df_y)
    valid_sets = [lgb_trainset]
    valid_names = ['train']
    gbm = lgb.train(
        params, 
        lgb_trainset,
        valid_sets=valid_sets,
        valid_names=valid_names,
    )
    return [
        gbm,
        {'score': 1337}
    ]


@node(inputs=['model', 'val_x'], outputs='result')
def predict(model, val_x):
    val_x['target'] = model.predict(val_x)
    return val_x

pipelines = dict(
    train=[train],
    infer=[predict]
)

mc = ModelChain(pipelines)

dwd_ds = Dataset(
    'DWD_ICON-EU',
     meta={'lat': 51, 'lon': 7}, 
     features=['Temperature'],
     start_date='2020-06-01',
     end_date='2020-08-03'
)
target_ds = Dataset('examples/STH-small.csv')

merged_ds = dwd_ds.merge(target_ds, method='dual-index-timeseries')

#print(dwd_ds.load())
#print(merged_ds.load())

#train_set, val_set = split(merged_ds, train_splits=10, val_splits=5)


train_inputs = {
    'train_set': merged_ds,
    'params': {

    }
}

result = mc.train(train_inputs)

dwd_ds_latest = Dataset(
    'DWD_ICON-EU',
     meta={'lat': 51, 'lon': 7}, 
     features=['Temperature'],
     start_date='2022-10-09 12:00',
     end_date='2022-10-13'
)
predict_inputs = {
    'val_x': dwd_ds_latest,
    'model': result['model']
}

result = mc.infer(predict_inputs)
df = result['result']
print(df)

