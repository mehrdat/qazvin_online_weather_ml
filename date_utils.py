from datetime import datetime,timezone


def timestamp2utc(ms:int|float)->str:
    return datetime.fromtimestamp(ms/1000, 
                                tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')


