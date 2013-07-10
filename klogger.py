import sys
import datetime

SHOWTIME = True

class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN= '\033[92m'
    OKPINK = '\033[95m'
    WARNING= '\033[93m'
    FAIL   = '\033[91m'
    ENDC   = '\033[0m'

class klog_levels:
    LOG_ERROR = 0
    LOG_WARN  = 1
    LOG_INFO  = 2
    LOG_DEBUG = 3

class klogger:

    def __init__(self, log_level = klog_levels.LOG_INFO):
        self.log_level = log_level

    def log_fatal(self, line):
        print self.rightnow() + bcolors.FAIL + "FATAL: " + str(line) + bcolors.ENDC
        sys.exit(-1)
    
    def log_error(self, line):
        print self.rightnow() + bcolors.FAIL + "ERROR: " + str(line) + bcolors.ENDC
    
    def log_warn(self, line):
        if self.log_level < klog_levels.LOG_WARN:
            return;
        print self.rightnow() + bcolors.WARNING + "WARN: " + bcolors.ENDC + str(line)
    
    def log_info(self, line):
        if self.log_level < klog_levels.LOG_INFO:
            return;
        print self.rightnow() + bcolors.OKGREEN + "INFO: " + bcolors.ENDC + str(line)
    
    def log_debug(self, debug_level, line): # debug levels are >= 1
        if self.log_level < klog_levels.LOG_DEBUG + debug_level - 1:
            return;
        print self.rightnow() + bcolors.OKPINK + "DEBUG (%d): " % debug_level + \
              bcolors.ENDC + str(line)
    
    def rightnow(self):
        if not(SHOWTIME):
            return ""
        return bcolors.OKBLUE + "[" + datetime.datetime.now().strftime("%H:%M:%S") \
               + "]" + bcolors.ENDC + " "
