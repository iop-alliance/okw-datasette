# Autor: Antonio de Jesus Anaya Hernandez, DevOps eng. for the IoPA
# Autor: The internet of production alliance, 2022
# Description: This script is part of the tools needed for the daatasette deployment of the OKW.


import sys


__title = "\n\n\t+-+-+-+-+-+-+-+-+\n\
\t|I|n|t|e|r|n|e|t|\n\
\t+-+-+-+-+-+-+-+-+\n\
\t|o|f|\n\
\t+-+-+-+-+-+-+-+-+-+-+\n\
\t|P|r|o|d|u|c|t|i|o|n|\n\
\t+-+-+-+-+-+-+-+-+-+-+\n\
\t|A|l|l|i|a|n|c|e|\n\
\t+-+-+-+-+-+-+-+-+\n\
"

__intro = "\n\t**This tool converts csv files to sqlite format.\n"


print("#"*60)
print(__title)
print("#"*60)
print(__intro)

try:
    import csv_to_sqlite as csq
    options = csq.CsvOptions(typing_style="full", encoding="UTF-8")
    __files = sys.argv[1:]
    if __files.__len__() > 0:
        for _file in __files:
            if _file[-4:] == ".csv":
                print("#"*60)
                print("\nFile(s): " + _file + "\n")
                print("#"*60)
                print("\n")
                if not csq.write_csv([_file], _file + ".sqlite", options):
                    print("\n")
                    print("#"*60)
                    print("\n\t**No valid File. Or sqlite file/table already exist.")
                    print("\n\t**Fix by deleting old sqlite files in the CSV file location\n")
                    print("#"*60)
            else:
                print("Invalid FILE use CSV file")
    else:
        print("\n**No file provided\nUse: okw_csv_sqlite.py database.csv\n")

except ModuleNotFoundError:
    print("\n**Error Execute: pip install csv_to_sqlite\n" )
