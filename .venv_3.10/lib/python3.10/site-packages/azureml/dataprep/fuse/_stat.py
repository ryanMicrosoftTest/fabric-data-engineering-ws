import os
from stat import S_IFDIR
from typing import Optional


def create_stat(mode,
                size: Optional[int],
                access_time: Optional[int],
                modified_time: Optional[int],
                change_time: Optional[int],
                permissions: Optional[int] = 0o555) -> os.stat_result:
    result = os.stat_result((mode | (permissions or 0o555),
                             0,
                             0,
                             2 if mode == S_IFDIR else 1,
                             0,
                             0,
                             size,
                             access_time,
                             modified_time,
                             change_time))
    return result


def update_stat(current: os.stat_result, new_size: int = None, new_atime: int = None) -> os.stat_result:
    result = os.stat_result((current.st_mode,
                             current.st_ino,
                             current.st_dev,
                             current.st_nlink,
                             current.st_uid,
                             current.st_gid,
                             new_size if new_size is not None else current.st_size,
                             new_atime if new_atime is not None else current.st_atime,
                             current.st_mtime,
                             current.st_ctime))
    return result


def stat_to_dict(stat: os.stat_result) -> dict:
    return {
        'st_mode': stat.st_mode,
        'st_size': stat.st_size,
        'st_atime': stat.st_atime,
        'st_mtime': stat.st_mtime,
        'st_ctime': stat.st_ctime
    }
