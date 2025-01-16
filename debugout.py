from enum import IntEnum

__debug_level = 1
PREFIX = '##'

class DebugLevels(IntEnum):
    VRBS = 0
    WRNG = VRBS + 1
    ERR = WRNG + 1

def set_dbglevel(level):
    global __debug_level
    __debug_level = level

def debugout(msg:str,level:int):
    if level < __debug_level:
        return
    match level:
        case DebugLevels.VRBS:
            print(f'{PREFIX} VERBOSE: {msg}')            
        case DebugLevels.WRNG:
            print(f'{PREFIX} WARNING: {msg}')
        case DebugLevels.ERR:
            print(f'{PREFIX} ERROR: {msg}')
        case _:
            raise Exception(f'Unknown level {level}')
