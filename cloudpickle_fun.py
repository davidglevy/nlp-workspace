# Databricks notebook source
from threading import Lock, Thread
from datetime import datetime
from threading import Lock

class LockWrapper:

    def __init__(self):
        self.lock = Lock()

    def __getstate__(self):
        return {}
    
    def __setstate__(self):
        self.lock = Lock()

class MySingleton:
    _instance = None
    _mylock = LockWrapper()

    def __new__(cls):
        if cls._instance is None: 
            with cls._mylock.lock:
                # Another thread could have created the instance
                # before we acquired the lock. So check that the
                # instance is still nonexistent.
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.creation_ts = datetime.now()

# COMMAND ----------

import time
singleton1 = MySingleton()
time.sleep(1)
singleton2 = MySingleton()

print(f"Creation Time for 1: {singleton1.creation_ts}")
print(f"Creation Time for 2: {singleton2.creation_ts}")
assert(id(singleton1) == id(singleton2))

# COMMAND ----------

import cloudpickle

cloudpickle.dumps(MySingleton)

# COMMAND ----------

cloudpickle.dumps(singleton1)
