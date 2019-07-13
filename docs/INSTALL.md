Installation
============


Getting ForML
---------------

To install the pre-packaged version of ForML simply use `pip`:

    pip3 install forml
    
ForML has number of optional features with their own dependencies which can be pulled in during the installation like
this:

    pip3 install 'forml[stdlib,dask]'


Install the graphviz ecosystem: 

    apt-get install graphviz


Extra Features
--------------

| Feature  | Install Command                     | Description                                                  |
|----------|-------------------------------------|--------------------------------------------------------------|
| all      | `pip3 install 'forml[all]'`       | All extra features                                           |
| stdlib   | `pip3 install 'forml[stdlib]'`    | The standard operator and actor library shipped with ForML |
| dask     | `pip3 install 'forml[dask]'`      | The Dask runner                                              |
| graphviz | `pip3 install 'forml[graphviz]'`  | The Graphviz pseudo-runner (also requires `graphviz` binary) |
