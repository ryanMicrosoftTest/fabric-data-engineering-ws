# Copyright (c) Microsoft Corporation. All rights reserved.

from enum import Enum
from .engineapi.typedefinitions import ArchiveType


class ArchiveOptions:
    def __init__(self,
                 archive_type: ArchiveType,
                 entry_glob: str = None):
        """
        :param archive_type: The type of archive file.
        :param entry_glob: The glob pattern for entries in archive file.
        """
        self.archive_type = archive_type
        self.entry_glob = entry_glob
