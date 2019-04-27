ForML Example Project Implementing the Titanic Solution
=========================================================

Quick Start
-----------

To execute the `train` flow in the development mode:

1. Install latest version of `forml`.  
2. From this directory (root of this `titanic` project) execute:

       $ python3 setup.py train --runner dask
       
3. Alternatively display the runtime graph instead of executing using following command

        $ python3 setup.py train --runner graphviz
