"""
An ION Coverage Foreign Data Wrapper

"""

from multicorn import ForeignDataWrapper
from .utils import log_to_postgres
import random
from logging import WARNING
from logging import INFO
import os, sys, inspect
from coverage_model import SimplexCoverage

class CovFdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing an ION coverage data model.

    Valid options:
    """

    def __init__(self, fdw_options, fdw_columns):
        super(CovFdw, self).__init__(fdw_options, fdw_columns)
        self.k = fdw_options["k"]
        self.columns = fdw_columns

    def execute(self, quals, columns):

        log_to_postgres("INFO:"+str(columns), WARNING)

        for x in range(0, 10):
            line=[]
            for f in self.columns:
                v = random.randrange(1, 100, 1)
                if (f == "lat"):
                    line.append("25.5")   
                elif (f == "lon"):     
                    line.append("11.1")
                elif (f == "dataset_id"):      
                    line.append("d010")
                elif (f == "cond"):
                    line.append(str(v))
                elif (f == "temp"):          
                    line.append(str(v))
                elif (f == "geom"):          
                   line.append("0101000020E610000000000000000039400000000000002640")
                else:
                    line.append("0:"+f)


            if len(line) > len(self.columns):
                            log_to_postgres("There are more columns than "
                                            "defined in the table", WARNING)
            elif len(line) < len(self.columns):
                log_to_postgres("There are less columns than "
                                "defined in the table", WARNING)
            else:
                yield line[:len(self.columns)]
