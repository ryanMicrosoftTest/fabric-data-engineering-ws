# Column Name Sanitizer Module

def makeNamesUnique(names: [], offset=0) -> []:
    # We don't want to use any existing names as new unique names
    # and then rename a column that is not duplicated. e.g.
    #  X, X, X_1
    # should result in:
    #  X, X_2, X_1
    # not:
    #  X, X_1, X_1_1  <- renamed something that is not duplicated breaking references to X_1
    takenNames = set(names)
    seenNames = set()
    unames = []
    baseNameCounters = {}
    for i, name in enumerate(names):
        if name == "" or name == None:
            name = "Column" + str(i - offset + 1)
            if name in takenNames:
                name = _makeUnique(baseNameCounters, takenNames, name)
                takenNames.add(name)
                seenNames.add(name)
        else:
            if name in seenNames:
                name = _makeUnique(baseNameCounters, takenNames, name)
                takenNames.add(name)
            else:
                seenNames.add(name)
        unames.append(name)

    return unames


def _makeUnique(baseNameCounters, nameSet, name: str) -> str:
    uniqueName = name
    if name in nameSet:
        suffix = baseNameCounters[name] if name in baseNameCounters else 1
        while True:
            uniqueName = name + "_" + str(suffix)
            suffix += 1
            if uniqueName not in nameSet:
                break
        baseNameCounters[name] = suffix
    return uniqueName


def makeUnique(nameSet, name: str) -> str:
    return _makeUnique({}, nameSet, name)


def validateUnique(name: str, existingNames: []):
    if not makeUnique(existingNames, name) == name:
        raise ValueError('Column name "{0}" already exists.'.format(name), 'InvalidColumnName')

