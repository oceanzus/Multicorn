"""
An ION Coverage Foreign Data Wrapper

"""

import sys
import math
import numpy as np
import numpy
import string

from numpy.random import random

import simplejson
from gevent import server
from gevent.monkey import patch_all; patch_all()
from pyon.util.breakpoint import breakpoint
from coverage_model import SimplexCoverage, QuantityType, ArrayType, ConstantType, CategoryType
import gevent

from multicorn import ForeignDataWrapper
from .utils import log_to_postgres
from logging import WARNING

import random
from random import randrange

import datetime

class CovFdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing an ION coverage data model.

    Valid options:
    """

    def __init__(self, fdw_options, fdw_columns):
        super(CovFdw, self).__init__(fdw_options, fdw_columns)
        self.k = fdw_options["k"]
        self.cov_path = fdw_options["cov_path"]
        self.columns = fdw_columns

    def execute(self, quals, columns):
        log_to_postgres("LOADING Coverage", WARNING)
        log_to_postgres("coverage path:"+self.cov_path, WARNING)
        cov = SimplexCoverage.load(self.cov_path)
        log_to_postgres("DataFields:"+str(columns), WARNING)
        log_to_postgres("---------------------", WARNING)

        paramTemp = cov.get_parameter_values('temp').tolist()
        paramCond = cov.get_parameter_values('conductivity').tolist()
        paramTime = cov.get_parameter_values('time').tolist()
        paramLat = cov.get_parameter_values('lat').tolist()
        paramLon = cov.get_parameter_values('lon').tolist()

        baseDateTime = datetime.datetime(2011,2,11,1,1,1)

        for i in range(0,len(paramTime)):
            line=[]
            
            dt = baseDateTime + datetime.timedelta(0,paramTime[i])
            s = dt.strftime("%Y-%m-%d %H:%M:%S")

            for f in self.columns:
                if (f == "lat"):
                    line.append(paramLat[i]+(0.01*i))   
                elif (f == "lon"):     
                    line.append(paramLon[i]+(0.01*i))
                elif (f == "dataset_id"):      
                    line.append("d010")
                elif (f == "cond"):
                    line.append(paramCond[i])
                elif (f == "temp"):          
                    line.append(paramTemp[i])
                elif (f == "time"):          
                    line.append(s) 
                elif (f == "geom"):          
                   line.append("0101000020E610000000000000000039400000000000002640")
                


            if len(line) > len(self.columns):
                log_to_postgres("There are more columns than "
                                            "defined in the table", WARNING)
            elif len(line) < len(self.columns):
                log_to_postgres("There are less columns than "
                                "defined in the table", WARNING)
            else:
                yield line[:len(self.columns)]
