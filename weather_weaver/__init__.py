# TODO investigate issue link with the following
# https://github.com/geopandas/dask-geopandas/pull/113
# https://github.com/geopandas/dask-geopandas/issues/237
# https://github.com/dask/dask/issues/9072
# https://github.com/dask/distributed/issues/4508

# Current workaround is to call monkey patching function from the msgpack_numpy package
import msgpack  # noqa: F401
import msgpack_numpy as m

m.patch()
