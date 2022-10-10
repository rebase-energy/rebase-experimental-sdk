import subprocess
import logging

class InvalidMergeMethod(Exception):
    pass

def dual_index_merge(df1, df2):
    df2 = df2.reindex(index=df1.index.get_level_values("valid_datetime")) \
                            .interpolate() \
                            .set_index(df1.index)
    return df1.join(df2, how='inner')


merge_methods = {
    'dual-index-timeseries': dual_index_merge
}


def merge(df1, df2, method):
    try:
        func = merge_methods[method]
        return func(df1, df2)
    except KeyError:
        raise InvalidMergeMethod




def install_repo(name):
    #url = f"~/Github/{name}"
    logging.info(f"Installing {name}")
    url = f"git+ssh://git@github.com/{name}.git"
    subprocess.run(['pip', 'install', url])
    logging.info(f"{name} has been installed")
