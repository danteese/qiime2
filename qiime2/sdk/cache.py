import contextlib
import qiime2
import os
import sqlite3
import json
import numpy as np


class Cache():
    """
    Cache is a checker of the last processes in a QIIME2 pipeline
    taking account the main class of the files with Provenance.

    This is an early stage of developing this kind of functionality.
    The motivation is urged by the #480 issue in the platform. 
    """

    def __init__(self, config, path):
        self.config = config
        self.path = path

    # Database functions
    def connection(self):
        db = sqlite3.connect(self.path + '/cache.db')
        try:
            cur = db.cursor()
            cur.execute('''CREATE TABLE cache (
            UUID TEXT (50),
            inputs BLOB,
            actionType TEXT (20) NOT NULL,
            action TEXT (30),
            parameters BLOB);''')
            msg = True
        except:
            msg = False
            db.rollback()
        db.close()
        return msg

    def generateExecUUID(self):
        """
        In order to maintain a sequence about the execution
        this function generate one UUID for all the process reviewed
        If new instance of this cache is created it would be a different
        pipeline. 
        """

    def isActionOnDB(self, *args):
        record = np.asarray(args)
        comp = (record[0].name, json.dumps(record[0].inputs),
                record[0].actionType, record[0].action, json.dumps(record[0].params))
        db = sqlite3.connect(self.path + '/cache.db')
        cur = db.cursor()
        msg = ''
        try:
            with db:
                cur.execute('SELECT * FROM cache WHERE UUID=:UUID',
                            {'UUID': record[0].name})
            querie = cur.fetchall()
            for queries in querie:
                if comp == queries:
                    msg = True
                else:
                    msg = False
        except Exception as inst:
            msg = False
        return msg

    def saveAction(self, *args):
        record = np.asarray(args)
        db = sqlite3.connect(self.path + '/cache.db')
        cur = db.cursor()
        try:
            with db:
                cur.execute('''INSERT INTO cache VALUES (
                    :UUID,
                    :inputs,
                    :actionType,
                    :action,
                    :parameters)''', {'UUID': record[0].name, 'inputs': json.dumps(record[0].inputs), 'actionType': record[0].actionType, 'action': record[0].action, 'parameters': json.dumps(record[0].params)})
                msg = 'Action added'
        except Exception as inst:
            msg = inst
        return msg

    # Graph functions
    def graphReconstruction(self, file, path):
        """
        This function returns the full graph generated 
        by provenance file
        """

    def generateNode(self, *args):
        """
        Creates and mount a temporary node.
        It saves all the important information about provenance file. 
        """

    def compareRequirements(self):
        """
        Compatibility. 
        This function checks that the current node share the same 
        versions of the current system.
        """

    # No provenance FILE
    def checkMD5(self, origin, current):
        """
        This function checks if the MD5 of one file (origin)
        is the same as the current file (current)
        """


class YAMLParser():
    """
    Provenance YAML parser
    """

    def __init__(self, path):
        self.path = path


class ActionRecord():
    """
    Helper class to save the right information inside
    SQLite database.
    """

    def __init__(self, name, action, description, actionType, inputs, params, workingDir):
        self.name = name
        self.action = action
        self.description = description
        self.actionType = actionType
        self.inputs = inputs
        self.params = params
        self.workingDir = workingDir

    def __getstate___(self):
        return {
            'actionName': self.name,
            'actionID': self.action,
            'actionType': self.actionType,
            'description': self.description,
            'inputs': self.inputs,
            'params': self.params,
            'dir': self.workingDir
        }


def verify_cache_file(path):
    """
    Verify file inside path, else create
    """
    
    file = path + '/.cache/cache.db'

    if not os.path.exists(path + '/.cache'):
        os.makedirs(path + '/.cache')

    try:
        with open(file):
            print("Cache database already created.")
    except IOError:
        open(file, 'a').close()
        print("Cache file created successfully.")


def set_cache_state(path, value=1):
    """
    Save the current state of the cache.
    """
    file_path = path + '/.cache/state'
    try: 
        with open(file_path, 'w') as state:
            state.write(str(value))
    except IOError:
        f = open(file_path, 'a')
        f.write(str(value))
        f.close()


def get_cache_state(path):
    """
    Read state inside .cache this works like a 
    env variable. 
    """
    file_path = path + '/.cache/state'
    try:
        with open(file_path, 'r') as state:
            cache_state = state.read()
            return cache_state
    except IOError:
        return False



@contextlib.contextmanager
def work_cache(sg):
    provenance = {
        'inputs': {},
        'params': {},
        'action_type': 'import'
    }
    if get_cache_state(os.getcwd()):
        
        if sg[2] is not None:
            provenance['inputs'] = sg[2].inputs
            provenance['params'] = sg[2].parameters
            provenance['action_type'] = sg[2].action_type
        
        record = ActionRecord(sg[0], sg[1], sg[3], provenance['action_type'],
                              provenance['inputs'], provenance['params'], os.getcwd())
        cache = Cache('None', os.getcwd()+'/.cache')
        msg = cache.connection()
        if provenance['action_type'] == 'import':
            msg = cache.saveAction(record)
            print(msg)
        else:
            if msg:
                msg = cache.saveAction(record)
                print(msg)
            else:
                msg = cache.isActionOnDB(record)
                if msg:
                    print('Action alredy in chache')
                else:
                    msg = cache.saveAction(record)
                    print(msg)
    else:
        raise NotImplementedError('Please execute qiime cache activate')
    try:
        yield record
    finally:
        print("Finally working with Cache")
