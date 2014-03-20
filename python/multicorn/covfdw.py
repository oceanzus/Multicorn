"""
An ION Coverage Foreign Data Wrapper
"""

__author__ = 'abird'

import sys
import math
import numpy as np
import numpy
import string
import time
from numpy.random import random
import numexpr as ne

import simplejson
from gevent import server
from gevent.monkey import patch_all; patch_all()
from pyon.util.breakpoint import breakpoint
from pyon.util.file_sys import FileSystem, FS

import gevent

from multicorn import ColumnDefinition
from multicorn import ForeignDataWrapper
from multicorn import Qual
from multicorn.compat import unicode_
from .utils import log_to_postgres
from logging import WARNING

from coverage_model.search.coverage_search import CoverageSearch, CoverageSearchResults, SearchCoverage
from coverage_model.search.search_parameter import ParamValue, ParamValueRange, SearchCriteria
from coverage_model.search.search_constants import IndexedParameters

from coverage_model import SimplexCoverage, AbstractCoverage,QuantityType, ArrayType, ConstantType, CategoryType


import random
from random import randrange
import os 
import datetime

TIME = 'time'


class CovFdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing an ION coverage data model.
    Valid options:
    - time, inside the coverage model, shoulud always be seconds since 1900-01-01
    - add 2208988800, number of seconds between 1900-01-01 and 1970-01-01
    """

    def __init__(self, fdw_options, fdw_columns):
        super(CovFdw, self).__init__(fdw_options, fdw_columns)
        self.k = fdw_options["k"]
        self.cov_path = fdw_options["cov_path"]
        self.cov_id = fdw_options["cov_id"]
        self.columns = fdw_columns

    def execute(self, quals, req_columns):
        #WARNING:  qualField:time qualOperator:>= qualValue:2011-02-11 00:00:00
        #WARNING:  qualField:time qualOperator:<= qualValue:2011-02-12 23:59:59
        log_to_postgres("dir:"+os.getcwd())
        os.chdir("/Users/rpsdev/externalization")
        log_to_postgres("dir:"+os.getcwd())

        #cov_path = self.cov_path[:len(self.cov_path)-len(self.cov_id)]

        log_to_postgres("LOADING Coverage At Path: "+self.cov_path, WARNING)
        log_to_postgres("LOADING Coverage ID: "+self.cov_id, WARNING)

        log_to_postgres("here")
        cov = SimplexCoverage.load(self.cov_path)
        log_to_postgres("here now")

        #time_param = ParamValueRange(IndexedParameters.Time, (1, 10))
        #coverage_id_param = ParamValue(IndexedParameters.CoverageId, self.cov_id)
        #criteria = SearchCriteria([time_param, coverage_id_param])

        #search = CoverageSearch(criteria, order_by=[TIME])

        #results = search.select()

        #log_to_postgres("found cov id's:"+str(results.get_found_coverage_ids()))

        #cov = results.get_view_coverage(self.cov_id, cov_path)
        log_to_postgres(type(cov))
        #    def get_value_dictionary(self, param_list=None, start_index=None, end_index=None):
        #paramdata = cov.get_value_dictionary()
        

        for qual in quals:
            if (qual.field_name ==TIME):
                log_to_postgres(
                    "qualField:"+ str(qual.field_name) 
                    + " qualOperator:" + str(qual.operator) 
                    + " qualValue:" +str(qual.value), WARNING)

        cov_available = True 

        if cov_available:
            #log quals
            log_to_postgres("LOADING Coverage", WARNING)
            cov = SimplexCoverage.load(self.cov_path)

            #log_to_postgres("Coverage PARAMS: "+str(cov.list_parameters())+"\n", WARNING)
            log_to_postgres("DataFields Requested:"+str(req_columns)+"\n", WARNING)
            #log_to_postgres("TableFields:"+str(self.columns)+"\n", WARNING)

            #time param
            paramTime = cov.get_parameter_values(TIME)

            #mock data
            self.generateMockRealData(len(paramTime))
            self.generateMockTimeData(len(paramTime))
                
            #data object
            data = []

            #actual loop
            for param_item in self.columns:
                dataType = self.columns[param_item].type_name
                log_to_postgres("Field: "+param_item+" \t DataType: "+dataType, WARNING)
                colName = self.columns[param_item].column_name

                if colName in req_columns:
                    if (param_item == TIME):
                        paramFromCov = self.getTimes(paramTime)
                        data.append(paramFromCov)

                    elif (colName.find(TIME)>=0):    
                        data = self.appendMockDataBasedOnType(dataType,data)
                    else:
                        try:
                            paramFromCov = cov.get_parameter_values(param_item)
                            data.append(paramFromCov)
                            pass
                        except Exception, e:
                            data = self.appendMockDataBasedOnType(dataType,data)
                            pass

                else:                
                    data = self.appendMockDataBasedOnType(dataType,data)
            
            #create np array to return
            dataarray = np.array(data)
            #return
            return dataarray.transpose() 
 
    def appendMockDataBasedOnType(self,data_type,data):
        if (data_type == "real"):
            data.append(self.paramMockData)
        elif (data_type.startswith("timestamp")):
            data.append(self.paramMockTimeData)

        return data            
        #log_to_postgres("added mock data", WARNING)

    def generateMockRealData(self,data_length):
        start = time.time()
        self.paramMockData = np.repeat(0, [data_length], axis=0)
        elapsedGen = (time.time() - start)
        log_to_postgres("Time to complete MockData:"+str(elapsedGen), WARNING)   

    def generateMockTimeData(self,data_length):
        start = time.time()
        base = datetime.datetime(1970,1,1,1,1,1)
        arr = np.array([base + datetime.timedelta(seconds=i) for i in xrange(data_length)])
        self.paramMockTimeData = [datetime.datetime.strftime(e, "%Y-%m-%d %H:%M:%S") for e in arr]
        elapsedGen = (time.time() - start)
        log_to_postgres("Time to complete MockTimeData:"+str(elapsedGen), WARNING)          


    def getTimes(self,paramTime):
        base = datetime.datetime(2011,2,11,1,1,1)
        arr = np.array([base + datetime.timedelta(hours=i) for i in xrange(len(paramTime))])
        s = [datetime.datetime.strftime(e, "%Y-%m-%d %H:%M:%S") for e in arr]
        return s  