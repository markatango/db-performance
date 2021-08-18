#!/usr/bin/env python
#future comptability
from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)
from builtins import *

import json
import os
from os import path, environ, mkdir, getcwd
import sys
import inspect

import pyodbc
import numpy as np

from pathlib import Path

#standard python logging
import logging
logger = logging.getLogger(__name__)

class DB:
    def __init__(self, **kwargs):
        self.server = kwargs["host"]
        self.database = kwargs["database"]
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        self.driver = kwargs["driver"]
        self.connect_string = 'DRIVER='+self.driver+';SERVER='+self.server+';DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password
    

    def connect(self):
        try:
            self.dbcon = pyodbc.connect(self.connect_string)
            self.dbcur = self.dbcon.cursor()
            logger.info("Connected to :" + self.server)
            return self.dbcon, self.dbcur
        except Exception as e:
            logger.info("Failed to connect to: " + str(self.server))
            logger.info(e)
            exit()

    def query(self, sqlString):
        try:
            self.dbcur.execute(sqlString)
            self.dbcon.commit()
            logger.info("Excuted query: " + sqlString)
        except Exception as ex:
            logger.info("Failed to execute query: "+sqlString)

    def close(self):
        if self.dbcur:
            self.dbcur.close()
            logger.debug("closing DB cursor")
        if self.dbcon:
            self.dbcon.close()
        logger.debug("closing DB connection")
    
class DbDriver:
  def __init__(self, **kwargs):
    self.kwargs = kwargs
    self.N = kwargs["N"]
    self.M = kwargs["M"]
    del self.kwargs["N"]
    del self.kwargs["M"]
    self.db = DB(**self.kwargs)
    self.conn, self.cursor = self.db.connect()
    
    
  
  def run(self):
    for i in range(self.N):
      for j in range(self.M):
        res = np.sqrt(j*np.sqrt(2))
      queryString = f"INSERT INTO testtable (data1, data2, float1, name) VALUES ({i}, {j}, {res}, 'Sponges {i}_{j}');"
      self.cursor.execute(queryString)
      logger.info(queryString)
    self.conn.commit()
    self.db.close()
    
def main():
    config = {
        "host": "tcp:awta-iot-sql-server.database.windows.net",
        "port": 1433,
        "username":"awtaAdmin",
        "password":"M1340SViterbi5",
        "database": "threadtest",
        "driver": "{ODBC Driver 17 for SQL Server}"
    }
    
    dd = DbDriver(**config, N=10, M=5)
    dd.run()

    # create database
    # queryString = """
    #     CREATE DATABASE IF NOT EXISTS testdb;
    #     """
    # db.query( queryString)

    # create table
    # fields = {
        # "data1": "INT",
        # "data2": "INT",
        # "float1": "FLOAT",
        # "name" : "CHAR(32)"
    # }

    # queryString = "USE threadtest;"
    # db.query( queryString)
       
    # queryString = "CREATE TABLE IF NOT EXISTS testtable (id INT IDENTITY(1,1) PRIMARY KEY, "+', '.join('{} {} NOT NULL'.format(k,v) for k,v in fields.items())+ ')'

    # logger.info(queryString)
    # db.query(queryString)

    # fieldNames = ",".join([""+str(i)+"" for i in list(fields.keys())])

    # queryString = f"INSERT INTO testtable ({fieldNames}) VALUES (1, 2, 0.3, 'Sponges');"

    # logger.info(queryString)
    # db.query(queryString)

 

if __name__ == "__main__": 
  import logging
  import logging.handlers
  import sys

  #create local logger
  logger = logging.getLogger(__name__)
  LOG_TO_CONSOLE = True #else, logs to file
  

  if LOG_TO_CONSOLE:
      handler = logging.StreamHandler(stream=sys.stdout)
  else:
      handler = logging.handlers.RotatingFileHandler(__file__+'.log', maxBytes=5000000, backupCount=1)
  
  formatter = logging.Formatter(fmt='%(asctime)s %(name) -20s %(levelname)-9s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  handler.setFormatter(formatter)
  
  logging.root.addHandler(handler)
  #set the logging level for root logger
  logging.root.setLevel(logging.DEBUG)
 
  main()
  
