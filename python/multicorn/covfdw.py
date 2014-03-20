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

cov_fail_data_size = 10**5

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
        cov_available = False 
        try:
            log_to_postgres("LOADING Coverage", WARNING)
            cov = SimplexCoverage.load(self.cov_path)
            cov_available = True 
            log_to_postgres("Cov Type:"+type(cov))
        except Exception, e:
            log_to_postgres("failed to load coverage...:" + str(e),WARNING)
            #raise e


        #time_param = ParamValueRange(IndexedParameters.Time, (1, 10))
        #coverage_id_param = ParamValue(IndexedParameters.CoverageId, self.cov_id)
        #criteria = SearchCriteria([time_param, coverage_id_param])
        #search = CoverageSearch(criteria, order_by=[TIME])
        #results = search.select()
        #log_to_postgres("found cov id's:"+str(results.get_found_coverage_ids()))
        #cov = results.get_view_coverage(self.cov_id, cov_path)
        #    def get_value_dictionary(self, param_list=None, start_index=None, end_index=None):
        #paramdata = cov.get_value_dictionary()
        for qual in quals:
            if (qual.field_name == TIME):
                log_to_postgres(
                    "qualField:"+ str(qual.field_name) 
                    + " qualOperator:" + str(qual.operator) 
                    + " qualValue:" +str(qual.value), WARNING)
        
        log_to_postgres("DataFields Requested:"+str(req_columns)+"\n", WARNING)
        #log_to_postgres("TableFieldsAvailable:"+str(self.columns)+"\n", WARNING)
        if cov_available:
            #log_to_postgres("Coverage PARAMS: "+str(cov.list_parameters())+"\n", WARNING)
            log_to_postgres("TableFields:"+str(self.columns)+"\n", WARNING)
            #time param
            param_time = cov.get_parameter_values(TIME)
            #mock data
            self.generate_mock_real_data(len(param_time))
            self.generate_mock_time_data(len(param_time))
        else:
            #mock data
            log_to_postgres("added mock data:"+str(cov_fail_data_size), WARNING)
            self.generate_mock_real_data(cov_fail_data_size)
            self.generate_mock_time_data(cov_fail_data_size)
            param_time = self.param_mock_time_data
            
        #data object
        start = time.time()
        
        data = []
        #actual loop
        for param_item in self.columns:
            dataType = self.columns[param_item].type_name
            #log_to_postgres("Field: "+param_item+" \t DataType: "+dataType, WARNING)
            col_name = self.columns[param_item].column_name

            if col_name in req_columns:
                #if the field is time add it to the return block
                if (param_item == TIME):
                    param_from_cov = self.get_times(param_time)
                    data.append(param_from_cov)

                elif (col_name.find(TIME)>=0):    
                    data = self.append_mock_data_based_on_type(dataType,data)
                else:
                    try:
                        param_from_cov = cov.get_parameter_values(param_item)
                        data.append(param_from_cov)
                        pass
                    except Exception, e:
                        data = self.append_mock_data_based_on_type(dataType,data)
                        pass

            else:                
                data = self.append_mock_data_based_on_type(dataType,data)

        
        '''
        maybe?
        data = dict()
        for param_item in self.columns:
            data[self.columns[param_item].column_name] = self.get_data_values(param_item,req_columns,param_time)

        elapsedGen = (time.time() - start)
        log_to_postgres("Time to complete data generation:"+str(elapsedGen), WARNING)   
        
        return data
        '''
        
        #create np array to return
        dataarray = np.array(data)
        return dataarray.transpose()
        '''
        #chunking
        chunk_size = 1000;
        array_shape = dataarray.shape[0]
        num = array_shape/chunk_size
        num_count = int(math.floor(num))

        log_to_postgres(str(array_shape))
        log_to_postgres(str(dataarray.shape))
        
        if array_shape < 10000:
            return dataarray
        else:    
            log_to_postgres("chunk yield:" + str(num_count))
            for i in range(0,(num_count)):
                d =None
                if i ==0:
                    #log_to_postgres(str(i)+ " F:"+str(0)+" T:"+str((i*chunk_size)+chunk_size))
                    d =  dataarray[0:(i*chunk_size)+chunk_size, :]
                elif i < num_count-1:
                    #log_to_postgres(str(i)+ " F:"+str(i*chunk_size+1)+" T:"+str((i*chunk_size)+chunk_size))
                    d = dataarray[(i*chunk_size+1):(i*chunk_size)+chunk_size, :]
                else:
                    #log_to_postgres(str(i)+ " F:"+str(i*chunk_size)+" T: ALL")
                    d =  dataarray[i*chunk_size:, :]
                self.yield_data(d)

        log_to_postgres("complete...")
        '''
                
   
 
    '''
    def yield_data(self,d):
        yield d

    def get_data_values(self,param_item,req_columns,param_time):
        dataType = self.columns[param_item].type_name  
        col_name = self.columns[param_item].column_name       
        if col_name in req_columns:
                #if the field is time add it to the return block
                if (col_name == TIME):
                    return self.get_times(param_time)
                elif (col_name.find(TIME)>=0):    
                    return None
                elif (col_name.find("_lookup")>=0):    
                    return None    
                else:
                    try:
                        return cov.get_parameter_values(col_name)
                    except Exception, e:
                        if (data_type == "real"):
                            return self.paramMockData
                        elif (data_type.startswith("timestamp")):
                            return self.param_mock_time_data                        
        else:                
            return None
        '''    


    def append_mock_data_based_on_type(self,data_type,data):
        if (data_type == "real"):
            data.append(self.paramMockData)
        elif (data_type.startswith("timestamp")):
            data.append(self.param_mock_time_data)

        return data            
        #log_to_postgres("added mock data", WARNING)

    def generate_mock_real_data(self,data_length):
        start = time.time()
        self.paramMockData = np.repeat(0, [data_length], axis=0)
        elapsedGen = (time.time() - start)
        log_to_postgres("Time to complete MockData:"+str(elapsedGen), WARNING)   

    def generate_mock_time_data(self,data_length):
        #generate array of legnth
        #time is seconds since 1970-01-01 (if its a float)
        start = time.time()
        self.param_mock_time_data = np.array([1+i for i in xrange(data_length)])
        #base = datetime.datetime(1970,1,1,1,1,1)
        #arr = np.array([base + datetime.timedelta(seconds=i) for i in xrange(data_length)])
        #self.param_mock_time_data = [datetime.datetime.strftime(e, "%Y-%m-%d %H:%M:%S") for e in arr]
        elapsedGen = (time.time() - start)
        log_to_postgres("Time to complete MockTimeData:"+str(elapsedGen), WARNING)          

    #convert date time object to string
    def get_times(self,param_time):
        #date time float is seconds since 1970-01-01
        #formats the datetime string as  
        s = [datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(e*1000),"%Y-%m-%d %H:%M:%S") for e in param_time]
        return s  