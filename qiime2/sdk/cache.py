import contextlib
import qiime2
import os


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
        """
        Do something with SQLite
        """

    def generateExecUUID(self):
        """
        In order to maintain a sequence about the execution
        this function generate one UUID for all the process reviewed
        If new instance of this cache is created it would be a different
        pipeline. 
        """

    def isActionOnDB(self):
        """
        Query database and checks if the action info 
        is in there 
        """

    def saveAction(self, *args):
        """
        Save the important fingerprint of provenance node
        """

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


def verify_cache_dir(path): 
    print(path)
    if os.path.isdir(path + '/.cache'):
        print("Cache directory found.")
        return True
    return False


@contextlib.contextmanager
def work_cache(sg):
    # TODO Verify .cache dir
    # TODO Verify datase is inside
    if verify_cache_dir(os.getcwd()):
        print("Cache working")
    else:
        raise NotImplementedError('Mandatory to have .cache directory created')
    record = ActionRecord(sg[0], sg[1], sg[3], sg[2].action_type,
                          sg[2].inputs, sg[2].parameters, os.getcwd())
    print(record.name, record.action, record.description,
          record.actionType, record.inputs, record.params, record.workingDir)

    try:
        yield record
    finally:
        print("Finally working with Cache")
