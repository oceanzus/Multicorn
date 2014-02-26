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
from coverage_model import SimplexCoverage, QuantityType, ArrayType, ConstantType, CategoryType
import gevent

from multicorn import ColumnDefinition
from multicorn import ForeignDataWrapper
from multicorn import Qual
from multicorn.compat import unicode_
from .utils import log_to_postgres
from logging import WARNING

import random
from random import randrange
import os 
import datetime


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
        #self.time_field = fdw_options["time_field"]
        #self.latitude = fdw_options["latitude"]
        #self.longitude = fdw_options["longitude"]
        #self.timefields = fdw_options["longitude"]
        self.columns = fdw_columns

    def execute(self, quals, req_columns):
        #start = time.time()

        #WARNING:  qualField:time qualOperator:>= qualValue:2011-02-11 00:00:00
        #WARNING:  qualField:time qualOperator:<= qualValue:2011-02-12 23:59:59


        for qual in quals:
            log_to_postgres(
                "qualField:"+ str(qual.field_name) 
                + " qualOperator:" + str(qual.operator) 
                + " qualValue:" +str(qual.value), WARNING)

            '''
            if (str(qual.field_name) == self.time_field):
                if (qual.operator == '>='):
                    timeStart = qual.value
                elif (qual.operator == '<='):    
                    timeEnd = qual.value

                log_to_postgres("timefield is"+str(self.time_field), WARNING)
            '''

        cov_exist = os.path.exists(self.cov_path)
        cov_available = False
        if (cov_exist):
            log_to_postgres(str(self.cov_path), WARNING) 
            cov_available = True
            pass
        else:
            log_to_postgres("OMG ITS NOT THERE!", WARNING) 
            pass    


        if (cov_available):
            #log quals
            log_to_postgres("LOADING Coverage", WARNING)
            cov = SimplexCoverage.load(self.cov_path)
            
            log_to_postgres("Coverage PARAMS: "+str(cov.list_parameters())+"\n", WARNING)

            log_to_postgres("---------------------", WARNING)
            log_to_postgres("DataFields:"+str(req_columns)+"\n", WARNING)
            log_to_postgres("TableFields:"+str(self.columns)+"\n", WARNING)
            log_to_postgres("---------------------", WARNING)

            #time param
            paramTime = cov.get_parameter_values("time")

            #self.generateMockRealData(1**8)
            #self.generateMockTimeData(1**8)

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
                    if (param_item == "time"):
                        paramFromCov = self.getTimes(paramTime)
                        data.append(paramFromCov)

                    elif (colName.find('time')>=0):    
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

        log_to_postgres("added mock data", WARNING)

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

    def timeCode():
        '''
        elapsedGen = (time.time() - startGen)
        log_to_postgres("Time to complete TimeGen:"+str(elapsedGen), WARNING)

        elapsed = (time.time() - start)
        log_to_postgres("Time to complete:"+str(elapsed), WARNING)
        log_to_postgres("Time to complete (W/O Gen):"+str(elapsed-elapsedGen), WARNING)
        '''

    def updateDataParams():
        #generate a data object to return to postgres
        data = []
        for f in self.columns:
            if (f == "lat"):
                data.append(paramLat) 
            elif (f == "lon"):  
                data.append(paramLon)
            elif (f == "dataset_id"):    
                data.append(paramDataset_id)
            elif (f == "cond"):
                data.append(paramCond)
            elif (f == "temperature"):      
                data.append(paramTemp)
            elif (f == "pressure"):      
                data.append(paramPressure)    
            elif (f == "time"):          
                data.append(s)
            elif (f == "geom"):          
                data.append(paramGEOM)
        

    def getSimpleParams():
        #paramTime = cov.get_parameter_values('time')
        #paramTemp = cov.get_parameter_values('temperature')
        #paramPressure = cov.get_parameter_values('pressure')
        pass    

    def generateEmptyData():
        '''
        x1 = np.array("d010")
        paramDataset_id = np.repeat(x1, [len(paramTime)], axis=0)
        '''
        #
        '''
        x2 = np.array("0101000020E610000000000000000039400000000000002640")
        paramGEOM = np.repeat(x2, [len(paramTime)], axis=0)
        '''
        pass

    def joinData():
        pass
        

    def extractDataRange():
        '''
        
        bitmask = ne.evaluate("time >= 20 & time <= 90")
        paramTime = paramTime[bitmask]
        paramTemp = paramTemp[bitmask]
        paramCond = paramCond[bitmask]
        #paramLat = cov.get_parameter_values('lat')
        #paramLat = paramLat[bitmask]
        #paramLon = cov.get_parameter_values('lon')
        #paramLon = paramLon[bitmask]
        diff = np.linspace(0, 1, len(paramLon))
        paramLon = paramLon+diff
        '''
        pass    
