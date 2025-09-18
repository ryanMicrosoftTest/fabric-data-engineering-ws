import Sanitizer
# Parquet related function Module

def sanitizeParquetNames(names):
    t = str.maketrans(" ,;{}()\n\t=", "__________")
    return [name.translate(t) for name in names]

def makePathColumnUnique(df, pathColumnName='Path'):
    columnsNamesAndPath = [pathColumnName] + list(df.columns)
    uniquifiedNames = Sanitizer.makeNamesUnique(columnsNamesAndPath)
    uniquifiedNames.remove(pathColumnName)
    df.columns = uniquifiedNames
    return df
