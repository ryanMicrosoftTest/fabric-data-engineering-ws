import Sanitizer
import xlrd, datetime
import pandas, random
from collections import namedtuple

def readExcel(dataFrame, args):
    headerRow = ["Path"]
    rows = []

    # Try to extract path from reader arguments, CLex passes paths this way until a CLex <=> Python stream exists.
    path = args.path if hasattr(args, 'path') and args.path != None else None
    # If no path argument then this is being used for python export and path(s) should be in the passed dataframe.
    if (path == None):
        path = dataFrame.iloc[0]['Path'] # For now we don't support mulltifile excel load.

    with xlrd.open_workbook(path) as xlsx:
        # If the sheet name is null or empty, we default to the first sheet.
        try: 
            if args.sheetName:
                sheet = xlsx.sheet_by_name(args.sheetName)
            else:
                sheet = xlsx.sheet_by_index(0)
        except:
           if len(xlsx.sheet_names()) < 1:
               raise ValueError("The provided spreadsheet contains no valid sheets.")
           else:
               raise ValueError("Unable to retrieve the sheet with name '%s'" % args.sheetName)

        dataRowIndex = 0
        for i, row in enumerate(sheet.get_rows()):
            # fix up types
            #   XL_CELL_EMPTY       0   empty string
            #   XL_CELL_TEXT        1   a Unicode string
            #   XL_CELL_NUMBER      2   float
            #   XL_CELL_DATE        3   float
            #   XL_CELL_BOOLEAN     4   int; 1 means TRUE, 0 means FALSE
            #   XL_CELL_ERROR       5   int representing internal Excel codes
            for j, cell in enumerate(row):
                if cell.ctype == 0:
                    row[j] = None
                elif cell.ctype == 1:
                    row[j] = cell.value if isinstance(cell.value, str) else str(cell.value)
                elif cell.ctype == 2:
                    row[j] = str(int(cell.value)) if cell.value.is_integer() else str(cell.value)
                elif cell.ctype == 3:
                    try:
                        dt_tuple = xlrd.xldate_as_tuple(cell.value, xlsx.datemode)
                        row[j] = str(datetime.datetime(*dt_tuple))
                    except:
                        row[j] = str(cell.value)
                elif cell.ctype == 4:
                    row[j] = 'True' if cell.value == 1 else 'False'
                elif cell.ctype == 5:
                    row[j] = xlrd.error_text_from_code[cell.value]

            continueProcessingRows = FileHelper.appendRowToRowsPerSampleScheme(rows, [path] + row, None, None, dataRowIndex)
            if continueProcessingRows == False:
                break
            dataRowIndex = dataRowIndex + 1

    dfc = FileHelper.createDataFrameAndHeader(headerRow, rows)

    return dfc

class FileHelper():
    @classmethod
    def fileEncodingEnumToPythonEncodingString(cls, args):
        if hasattr(args, 'fileEncoding'):
            if args.fileEncoding == 0:
                return 'utf-8'
            if args.fileEncoding == 1:
                return 'iso-8859-1'
            if args.fileEncoding == 2:
                return 'latin-1'
            if args.fileEncoding == 3:
                return 'ascii'
            if args.fileEncoding == 4:
                return 'utf-16'
            if args.fileEncoding == 5:
                return 'utf-32'
            if args.fileEncoding == 6:
                return 'utf-8-sig'
        else:
            return None

    @classmethod
    def appendRowToRowsPerSampleScheme(cls, rows, row, sampleRandom, maxRows, dataRowIndex):
        if sampleRandom:
            if len(rows) < maxRows:
                rows.append(row)
            else:
                r = int(random.random() * (dataRowIndex + 1))
                if r < maxRows:
                    rows[r] = row
        elif maxRows:
            rows.append(row)
            if len(rows) >= maxRows:
                return False
        else:
            rows.append(row)

        return True

    @classmethod
    def createHeaders(cls, headerRow, numColumns):
        headers = [hrc or 'Column' + str(hri) for hri, hrc in enumerate(headerRow)] + (['Column' + str(c + len(headerRow)) for c in range(0, numColumns - len(headerRow))])
        headers = Sanitizer.makeNamesUnique(headers)
        return headers

    @classmethod
    def createDataFrameAndHeader(cls, headerRow, rows):
        if len(headerRow) > 0 and len(rows) == 0:
            rows.append(headerRow)
            headerRow = []

        df = pandas.DataFrame(rows)
        headers = FileHelper.createHeaders(headerRow, len(df.columns))

        # compensate for a Pandas issue where the header row can't be wider than the dataframe
        # if so, pad the first row with blanks, then re-create dataframe
        if len(headers) > len(df.columns):
            rows[0].extend([None] * (len(headers) - len(rows[0])))
            df = pandas.DataFrame(rows)

        df.columns = headers

        return df
