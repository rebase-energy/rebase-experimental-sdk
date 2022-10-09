from functools import wraps


def node(**kwargs):
  inputs = kwargs.get('inputs')
  outputs = kwargs.get('outputs')
  def wrapper(f):
      
      @wraps(f)
      def inner(*args, **kwargs):

        # if we are calling the function to inspect kwargs, 
        # then just return function and kwargs without executing
        if kwargs and kwargs.get('_internal_inspect'):
          return inner, inputs, outputs

        return f(*args, **kwargs)
      
      return inner
  return wrapper
