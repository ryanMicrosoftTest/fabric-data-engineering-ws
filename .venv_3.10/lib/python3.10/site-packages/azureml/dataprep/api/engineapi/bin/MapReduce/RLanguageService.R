# This R language service is out of date with the format of data sent and received.
# it will need to be updated before turning on the R language service. File is kept
# checked in as a starting point.

library(rjson)

MapRows = function (functions, schema, rows, columns)
{
    df = data.frame(columns, stringsAsFactors=FALSE)
    names(df) = schema

    newSchema = list()
    newColumns = list()
    length(newColumns) = length(functions)
    length(newSchema) = length(functions)

    for (i in 1:length(functions))
    {
        newSchema[[i]] = paste("C", i, sep="")

        if (rows > 0)
        {
            newColumn = eval(parse(text=functions[[i]]), list(row=df))
        }
        else
        {
            newColumn = list()
        }

        if (length(newColumn) == 1)
        {
            newColumn = list(newColumn)
        }

        newColumns[[i]] = newColumn
    }

    list(newSchema, rows, newColumns)
}

Process = function (input)
{
    message = fromJSON(input)

    if (message$Rows == 1)
    {
        for (i in 1:length(message$Columns))
        {
            message$Columns[i] = list(message$Columns[i])
        }
    }

    if (message$Operation == "MapRows")
    {
        result = MapRows(message$Functions, message$Schema, message$Rows, message$Columns)
    }
    else if (message$Operation == "MapPartition")
    {
        stop("MapPartition not implemented")
    }
    else
    {
        stop(paste("Unknown operation:", message$Operation))
    }
    
    output = toJSON(list("Success", result))
}

OnError = function (exception)
{
    toJSON(list("Error", exception$message))
}

stdin = file("stdin", "r")
while (TRUE) 
{
    input = readLines(stdin, n=1)

    if (length(input) == 0) 
    {
        break
    }

    output = tryCatch(Process(input), error = OnError)

    writeLines(output)
}
