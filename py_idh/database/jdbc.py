from py_idh import database
import time
import re
import yaml
import json
import traceback
import uuid
import websockets
import asyncio
from requests import Session
import pandas as pd
from pathlib import Path
from threading import Thread
import sys

from ..core.singleton_class import Singleton
import py_idh.container as container

# error handling and logging
from ..core.logging import error_handler, logging

# import 
import python_jdbc.sql_generator.sql_snippets as sql_snippets
import python_jdbc.sql_generator.alter_table as alter_table

def start_background_loop(_loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

@Singleton
class PythonJdbc():

    def __init__(self, **args):
        self.logging_label = 'idh_jdbc'
        self._javaHost = args.get('java_host', None)
        self._javaPort = args.get('java_port', None)
        self._runningTasks = {}
        self._finishedTasks = {}
        self._clientId = str(uuid.uuid4())
        self._lastPing = time.time()
        self._pingInterval = 10 # every 10 seconds we need a ping
        self._ws = None
        self._wsRegistered = False
        self.session = Session()
        
    def load_config(self):
        self._javaHost = self._javaHost or container.javaHost
        self._javaPort = self._javaPort or container.javaPort

    @property
    def java_ready(self):
        return self._wsRegistered 

    async def init (
        self,
        loop,
        isReconnect = False):
        self.load_config()
        asyncio.set_event_loop(loop)
        tasks = []
        # only execute this code during re-connectsnot 
        # if reconnect, but no tasks in line -> wait and try again after 2 seconds
        if isReconnect and not self._runningTasks:
            time.sleep(2)
            await self.init(loop,
                isReconnect)
            return

        # _ws will be reset during socket close
        if not self._ws:
            if isReconnect:
                asyncio.ensure_future(self._init_ws(loop), loop = loop)
            else:
                tasks.append(asyncio.ensure_future(self._init_ws(loop)))
        if tasks:
            loop.run_until_complete(asyncio.wait(tasks))
        counter = 0
        while not self.java_ready and counter < 120: # wait for max one minute
            time.sleep(0.5)
            counter += 1
        
        if counter == 120:
            error_handler(f"Could not reach Java Server via websockets", None, self.logging_label)

    # def create_schema(
    #     self,
    #     schema_name = None,
    #     connection_id = None,
    #     token = None,
    #     host = None,
    #     port = None,
    #     jdbc_token = None,
    #     connection_data = None):

    #     self.token = token

    #     #TODO: query = ...

    #     task_data = {
    #         'taskId': str(uuid.uuid4()),
    #         'command': 'execute',
    #         'params': {
    #             'query': query}}
    #     if connection_id:
    #         task_data['connectionId'] = connection_id 
    #     else:
    #         task_data['connectionData'] = connection_data 
    #     if host and port:
    #         task_data.update({'host': host, 'port': port})
    #     if jdbc_token:
    #         task_data['jdbc_token'] = jdbc_token
    #     return self._addTask(task_data)

    def has_table(
        self,
        schema_name = None,
        table_name = None,
        connection_id = None,
        token = None,
        host = None,
        port = None,
        jdbc_token = None,
        connection_data = None):

        self.token = token
        database_type = connection_data['database']
        query = sql_snippets.has_table(table=table_name, schema=schema_name)

        task_data = {
            'taskId': str(uuid.uuid4()),
            'command': 'execute',
            'params': {
                'query': query[database_type],
                }
            }
        if connection_id:
            task_data['connectionId'] = connection_id 
        else:
            task_data['connectionData'] = connection_data 
        if host and port:
            task_data.update({'host': host, 'port': port})
        if jdbc_token:
            task_data['jdbc_token'] = jdbc_token
        return self._addTask(task_data)

    def create_table(
        self,
        schema_name = None,
        table_name = None,
        columns = None,
        connection_id = None,
        token = None,
        host = None,
        port = None,
        jdbc_token = None,
        connection_data = None):

        self.token = token
        database_type = connection_data['database']
        query = alter_table.create_table(table=table_name, schema=schema_name, columns=columns)

        task_data = {
            'taskId': str(uuid.uuid4()),
            'command': 'execute',
            'params': {
                'query': query[database_type],
                }
            }
        if connection_id:
            task_data['connectionId'] = connection_id 
        else:
            task_data['connectionData'] = connection_data 
        if host and port:
            task_data.update({'host': host, 'port': port})
        if jdbc_token:
            task_data['jdbc_token'] = jdbc_token
        return self._addTask(task_data)


    def execute(
        self,
        query, 
        connection_id = None,
        token = None, 
        host = None,
        port = None,
        limit = None,
        jdbc_token = False,
        connection_data = None): 
        """
        send web request to JAVA Server to start run sql statement

        e.g.
        ```
        from py_idh.database import PythonJdbc
        user_token = "..." 
        PythonJdbc.execute("SELECT TOP 3* FROM dbo.BSEG", connection_id = ..., token = user_token)
        ```
        
        :param query: Sql statement string (only one statement allowed at once e.g. in HANA database)
        :param connection_id: connection id as saved in idh
        :param token: your dh-token
        :param host: idh server host
        :param port: idh server port
        :param limit: optional limit to a query to be set
        :param jdbc_token: jdbc server token to send task directly to jdbc server instead of idh server
        :param connection_data: if jdbc_token is provided - here you put a dictionary with the connection details

        :returns: query result as dataframe or None if execution without result set
        """
        self.token = token
        task_data = {
            'taskId': str(uuid.uuid4()),
            'command': 'execute',
            'params': {
                'query': query,}}
        if connection_id:
            task_data['connectionId'] = connection_id 
        else:
            task_data['connectionData'] = connection_data 
        if host and port:
            task_data.update({'host': host, 'port': port})
        if jdbc_token:
            task_data['jdbc_token'] = jdbc_token
        if limit:
            task_data['params']['limit'] = limit
        return self._addTask(task_data)

    def execute_batch (
        self,
        query, 
        params,
        connection_id = None,
        token = None, 
        host = None,
        port = None,
        jdbc_token = False,
        connection_data = None):  
        """
        send web request to JDBC Server to run batch statement and returns update counts for each statement (this means array of ones with length the number of rows in the matrix)

        e.g.
        ```
        from py_idh.database import PythonJdbc
        user_token = "..." 
        param_list = [[1,2], [3,4]]
        PythonJdbc.execute_batch("INSERT INTO dbo.MINI_TABLE VALUES(?, ?)", params = param_list, connection_id = ..., token = user_token)
        ```
        
        :param query: Sql statement string (only one statement allowed at once e.g. in HANA database), with as many ? in it as every entry in the param list
        :param params: 2d list of parameters 
        :param connection_id: connection id as saved in idh
        :param token: your dh-token
        :param host: idh server host
        :param port: idh server port
        :param jdbc_token: jdbc server token to send task directly to jdbc server instead of idh server
        :param connection_data: if jdbc_token is provided - here you put a dictionary with the connection details

        :returns: a dataframe with column 'result' and integers 1/0 for each iterations success/failure
        """
        self.token = token
        task_data = {
            'taskId': str(uuid.uuid4()),
            'command': 'executeBatch',
            'params': {
                'query': query,
                'params': params}}
        if connection_id:
            task_data['connectionId'] = connection_id 
        else:
            task_data['connectionData'] = connection_data 
        if host and port:
            task_data.update({'host': host, 'port': port})
        if jdbc_token:
            task_data['jdbc_token'] = jdbc_token
        return self._addTask(task_data)

    def execute_script (
        self,
        script_id,
        section_ids = None,
        target_branch = 'master',
        connection_id = None,
        token = None, 
        limit = None,
        host = None,
        port = None,
        connection_data = None):
        """
        send web request to JDBC Server to run sections in script, if no section ids are given all checked sections are executed
        e.g.
        ```
        from py_idh.database import PythonJdbc
        user_token = "..." 
        PythonJdbc.execute_script(script_id = ..., section_ids = [.,., ...], connection_id = ..., token = user_token)
        ```
        
        :param script_id: IDH sql script id
        :param section_ids: list of section ids, if not given (or empty) all sections that are checked are executed
        :param target_branch: git branch to execute from
        :param connection_id: connection id as saved in idh
        :param token: your dh-token
        :param limit: optional limit to a query to be set
        :param host: idh server host
        :param port: idh server port
        :param connection_data: if jdbc_token is provided - here you put a dictionary with the connection details

        :returns: a dataframe with the result of the last section that is executed
        """
        self.token = token
        params = {'scriptId': script_id, 'targetBranch': target_branch}
        if limit:
            params['limit'] = limit
        if section_ids:
            params['sectionIds'] = section_ids
        task_data = {
            'taskId': str(uuid.uuid4()),
            'command': 'executeScript',
            'returnDirectly': True,
            'params': params}
        if connection_id:
            task_data['connectionId'] = connection_id 
        else:
            task_data['connectionData'] = connection_data 
        if host and port:
            task_data.update({'host': host, 'port': port})
        return self._addTask(task_data)

    # init web sockets
    async def _init_ws(self, loop, **kw):
        logging('info', self.logging_label, f"Initializing web socket on '{self._javaHost}:{self._javaPort}'..." )
        try:
            async with websockets.connect(f"ws://{self._javaHost}:{self._javaPort}/jdbc-server/websocket") as websocket:
                self._ws = websocket
                await self._ws.send(json.dumps({'clientId': self._clientId}))
                logging('info', self.logging_label, f"The connection is open, ID: '{self._clientId}'" )
                # start heartbeat check
                asyncio.ensure_future(self._heartbeat_loop(), loop = loop)
                # set ping intervall
                self._lastPing = time.time()
                asyncio.ensure_future(self._ping_loop(), loop = loop)
                while True:
                    new_data = await self._ws.recv()
                    if new_data == 'registered':
                        logging('info', self.logging_label, f"Successfully registered at JDBC Server, ID: '{self._clientId}'" )
                        self._wsRegistered = True
                        self._lastPing = time.time()
                    else:
                        self._lastPing = time.time()
                        self._msgHandler(data = new_data)
        except websockets.ConnectionClosed as exc:
            logging('info', self.logging_label, f"The connection is closed, ID: '{self._clientId}', {str(exc)}" )
            # clear ping timeout for broken ws
            self._lastPing = time.time()
            # remove old socket
            self._ws = None
            # create a new socket
            kw['isReconnect'] = True
            await self.init(loop, **kw)
        except Exception as err:
            if self._ws:
                # handle error
                logging('error', self.logging_label, f"The connection broke, ID: '{self._clientId}', Error: {str(err)}")
                logging('debug', self.logging_label, traceback.format_exc())
                # terminate the broken ws
                await self._ws.close()
            else:
                await asyncio.sleep(5)
                # try again to connect 
                logging('warn', self.logging_label, "attempt to reconnect with java through websockets failed")
                kw['isReconnect'] = True
                await self.init(loop, **kw)

    async def _heartbeat_loop(self):
        """
        ping at most self._pingInterval seconds after last message from java
        """
        while self._ws and time.time() - self._lastPing < self._pingInterval * 2:
            wait_for = self._pingInterval * 2 - (time.time() - self._lastPing)
            await asyncio.sleep(wait_for)
        if self._ws:
            try:
                await self._ws.close()
            except:
                pass

    async def _ping_loop(self):
        """
        ping at most self._pingInterval seconds after last message from java
        """
        while self._ws:
            try:
                while time.time() - self._lastPing < self._pingInterval:
                    await asyncio.sleep(self._pingInterval - (time.time() - self._lastPing))
                pong_waiter = await self._ws.ping()
                await pong_waiter
                self._lastPing = time.time()
                await asyncio.sleep(self._pingInterval)
            except:
                logging('error', self.logging_label, f'problem with ping/pong \n{traceback.format_exc()}')
                if self._ws:
                    await self._ws.close()


    # ensure that client has been initialized
    def _checkInitState (self):
        if not self._javaHost or not self._javaPort:
            error_handler('Please initiate client first using the init function', None, self.logging_label)

    def _msgHandler (self, data = None): 
        msg = {}
        try:
            msg = json.loads(data)
            
            # change property frameNb from string to number
            msg['frameNb'] = int(msg['frameNb'])
        
            # if running task not found, throw error
            if msg['taskId'] not in self._runningTasks:
                error_handler(f"Task id '{msg['taskId']}' does not exist in running tasks", None, self.logging_label)
            # only for logging first entry
            # if msg['frameNb'] == 1:
            #     logging('info', self.logging_label, f"Task with id '{msg['taskId']}', received first frame ({msg['frameNb']}/{msg['totalNbFrames']} frames)")
            
            # aggregate frames to running task's result
            if msg['frameNb'] <= msg['totalNbFrames']:
                # logging('debug', self.logging_label, "Task with id "{msg['taskId']}", received {msg['frameNb']} / {msg['totalNbFrames']} frames" })
                if 'result' in msg and msg['result']:
                    self._runningTasks[msg['taskId']]['sqlResult'] += msg['result']
            
            # when all frames arrived, process the result
            if msg['frameNb'] >= msg['totalNbFrames']:
                is_stream = msg.get('isStream', False) or 'streamedPartitions' in self._runningTasks[msg['taskId']]
                if not is_stream:
                    logging('debug', self.logging_label, f"Received last frame ({msg['frameNb']}/{msg['totalNbFrames']} frames)")
                msgObject = json.loads(self._runningTasks[msg['taskId']]['sqlResult']) if 'sqlResult' in self._runningTasks[msg['taskId']] else {}
                self._runningTasks[msg['taskId']]['sqlResult'] = ''
                description = msg['description'] if 'description' in msg else None
                # { result?: { result: string[][], colTypes: string[], colNames: string[], precision: number[]}, error?: string }
                if 'error' in msg and msg['error']:
                    # this error only occurs on authorized error
                    self._finishedTasks[msg['taskId']] = { 
                        'status': 'error', 
                        'error': msg['error']}
                    logging('error', self.logging_label, msg['error'])
                elif 'error' in msgObject and msgObject['error']:
                    self._finishedTasks[msg['taskId']] = { 
                        'status': 'error', 
                        'error': msgObject['error']}
                    logging('error', self.logging_label, msgObject['error'])
                else:
                    # combine column name and value to return
                    result = None
                    if 'result' in msgObject:    
                        if self._runningTasks[msg['taskId']]['command'] == 'executeBatch':
                            result = pd.DataFrame([[entry] for entry in msgObject['result']], columns = ['result'])
                        else:
                            result = pd.DataFrame.from_records(data = msgObject['result'] if msgObject['result'] else [], columns = msgObject['colNames'])  
                    if is_stream or 'streamedPartitions' in self._runningTasks[msg['taskId']]:
                        if not msgObject.get('endOfStream'):
                            if 'streamedPartitions' not in self._runningTasks[msg['taskId']]:
                                self._runningTasks[msg['taskId']]['streamedPartitions'] = []
                            self._runningTasks[msg['taskId']]['streamedPartitions'].append(result)
                            if msgObject.get('streamPartitionNb'):
                                if msgObject.get('streamPartitionNb') % 50 == 0:
                                    logging('info', self.logging_label, f"Received streaming partition nb {msgObject.get('streamPartitionNb')}" + 
                                        (f", {msgObject['nbRows']} rows streamed so far" if msgObject.get('nbRows') else ''))
                                elif msgObject.get('streamPartitionNb') == 1:
                                    logging('info', self.logging_label, f"Data stream initiated")
                        else:
                            cumulated_result = pd.concat(self._runningTasks[msg['taskId']]['streamedPartitions'] + [result])
                            logging('info', self.logging_label, f"Successfully finished task")
                            self._finishedTasks[msg['taskId']] = {
                                'status': 'success', 
                                'result': cumulated_result}
                            del self._runningTasks[msg['taskId']]
                    else:
                        self._finishedTasks[msg['taskId']] = {
                            'status': 'success', 
                            'result': result}
                        logging('info', self.logging_label, f"Successfully finished task")
                if not is_stream and msg['taskId'] in self._runningTasks:
                    del self._runningTasks[msg['taskId']]
        except Exception as e:
            logging('error', self.logging_label, str(e) )
            logging('debug', self.logging_label, traceback.format_exc())
            # emit finish event and remove task from running tasks
            if 'taskId' in msg and msg['taskId']:
                self._finishedTasks[msg['taskId']] = { 
                    'status': 'error', 
                    'error': str(e) or 'unknown error' }
                if msg['taskId'] in self._runningTasks:
                    del self._runningTasks[msg['taskId']]

    def _addTask (self, taskData, attemptNb = 0): 
        self._checkInitState()
        taskData['clientId'] = self._clientId
        taskData['useRedis'] = False
        # store task for future event name
        self._runningTasks[taskData['taskId']] = {
            **taskData,
            'sqlResult': ''}

        # add event listener for waiting result from java server
        try:
            jdbc_token = taskData.pop('jdbc_token', None)
            if jdbc_token:
                port = taskData.pop('port', None)
                host = taskData.pop('host', None)
                headers = { 'authorization': jdbc_token, 'Content-type': 'application/json' }
                resp = self.session.post(f"http://{host or self._javaHost}:{port or self._javaPort}/jdbc-server/addTask", data=json.dumps(taskData), headers = headers , timeout = 36000)
            else:
                headers = { 'authorization': self.token, 'Content-type': 'application/json' }
                port = taskData.pop('port', None)
                host = taskData.pop('host', None)
                data = {'taskData': taskData}
                if taskData.get('connectionId'):
                    data['connectionId'] = taskData.pop('connectionId')
                resp = self.session.post(f"http://{host or container.nodeHost}:{port or container.nodePort}/api/external/run-sql-statement", data=json.dumps(data), headers = headers , timeout = 36000)               
            # resp.raise_for_status()
            if resp.status_code >= 400:
                raise Exception(f"bad request, status {resp.status_code} (reason: '{resp.reason}')")
            elif not jdbc_token and resp.json().get('error'):
                raise Exception(f"task cannot be transmitted to jdbc server, error: \n'{resp.json()['error']}'")
            # wait for result from ws
            if not taskData.get('returnDirectly'):
                counter = 0
                while taskData['taskId'] not in self._finishedTasks and counter < 10 * 60 * 60 * 10:
                    # five hours maximal wait period
                    time.sleep(0.02)
                    counter += 1
                result = self._finishedTasks.pop(taskData['taskId'])
                if result.get('error'):
                    raise Exception(f"Java-Server Error: \n'{result['error']}'")
                else:
                    return result['result']
            else:
                result = resp.json()
                if result.get('error'):
                    raise Exception(f"IDH Error: \n'{result['error']}'")
                else:
                    if isinstance(result.get('data'), str):
                        print(result['data'])
                    else:
                        return pd.DataFrame.from_records(data = result['data']['rows'] if result['data'].get('rows') else [], columns = result['data']['columns']) 
            
        except Exception as err:
            if ('ECONNRESET' in str(err) or 'is not registered as websocket client' in str(err)) and attemptNb < 4:
                # retry after waiting a little, there are too many tasks running
                time.sleep(1)
                attemptNb += 1
                return self._addTask(taskData, attemptNb)
            else:
                if str(err).startswith('bad request') or str(err).startswith('Java-Server Error'):
                    raise Exception(str(err))
                else:
                    error_handler(f"error in web request", traceback.format_exc(), self.logging_label)
           