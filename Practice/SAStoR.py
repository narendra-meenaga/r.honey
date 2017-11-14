# Author : Narendra K Meenaga
# Mail id : narendra.k.meenaga@gmail.com
# Date : Nov 5 , 2017
# Python 3.5

# Python script to convert SAS script to R

# Required Libraries
import re


#  Global Variables
SASinputScript = "D:/DataNotes/SAStoRinPython/SAS_Scripts/ex1.sas.txt"


stmtsList = []

# Read SAS file
sasFile     = open(SASinputScript,"r")
sasFileStr  = sasFile.read()

# Remove all kinds of comment lines
sasFileStr  = re.sub(r'(/\*(.|\n)*?\*/)|(\n+\s*\*(.|\n)*?;)','',sasFileStr)

# Split the .sas file into individual statements using split by ;
stmts     = sasFileStr.split(';')

# Trim the statement strings
for stmt in stmts:
    stmtsList.append(stmt.strip())
    
for stmt in stmtsList:
    print(stmt)    

