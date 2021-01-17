#!/usr/bin/python_64
# -*- coding: utf-8 -*-
import sybpydb
import sys
import os
import subprocess
import datetime
reload(sys)
sys.setdefaultencoding('utf8')
usage = "Usage:\n ./mmf_div_recon.py"
#print str(os.getenv('DSQUERY'))
#print str(os.getenv('SQL_USER'))
env = str(os.getenv('DSQUERY')) 
usr = str(os.getenv('SQL_USER'))
out = subprocess.check_output(["echo", env + "." + usr])
out = out.rstrip()

con = sybpydb.connect(
servername=str(os.getenv('DSQUERY')),
user= str(os.getenv('SQL_USER')),
password = subprocess.check_output(["secget", str(os.getenv('DSQUERY')) + "." + str(os.getenv('SQL_USER'))]).rstrip())

chg = con.cursor()
chained = "SET CHAINED ON"
chg.execute(chained)

cursor = con.cursor()


q1 = "Select GETDATE()"
cursor.execute(q1)
rows = cursor.fetchall()
res = rows[0]
today_date = res[0]
cursor.connection.commit()
filepath = os.path.join(os.getenv('IW_WORK_OUT'), 'MMFDivRecon' + str(today_date.strftime('%m-%d-%y')) + ".csv")
f = open(filepath, 'w')
f.write(str(today_date.strftime('%m-%d-%y')) + ",                           CI Investments,                                   Page 1\n")
f.write("                                                ,FUND DIVIDEND UPDATE\n                                              ,LIST OF EXCEPTIONS\n\t,\n")
f.write("Parent_Fund_Code," + "Main_Fund_Code," +  "Fund_Name" + (" " * 16) + "," + "Unit_Distr_Cap," + "Unit_Distr_Income,"  + "T_Fund_Flag,"  +  "Corp_Trust_Code,"  + "Fund_Type" + "\n")


q2 = "CREATE TABLE ##All_Distributions(Main_Fund_Code Fund_Code_Class, Unit_Distr_Cap Fractional_Dollar_Class NULL, Unit_Distr_Income Fractional_Dollar_Class NULL, Parent_Fund_Code Fund_Code_Class) "
cur2 = con.cursor()
cur2.execute(q2)
cur2.connection.commit()

q3="""INSERT INTO ##All_Distributions
SELECT DISTINCT a.Main_Fund_Code, Unit_Distr_Cap, Unit_Distr_Income, Parent_Fund_Code 
FROM Fund_Div_Distr a, Fund_Map b, Fund c, System_Control sc
WHERE b.Main_Fund_Code = a.Main_Fund_Code AND
c.Main_Fund_Code = a.Main_Fund_Code AND
a.Distr_Date = dbo.UNBD_udf(sc.Last_Posting_Date,1)  AND
c.Fund_Type = 'MMF' AND
c.Product_Type_Code = 'M'"""
cur3 = con.cursor()
cur3.execute(q3)
cur3.connection.commit()

q4="CREATE TABLE ##PF_Codes(Parent_Fund_Code Fund_Code_Class, Main_Fund_Code Fund_Code_Class) "
cur4 = con.cursor()
cur4.execute(q4)
cur4.connection.commit()

q5="""INSERT INTO ##PF_Codes
SELECT DISTINCT b.Parent_Fund_Code, b.Main_Fund_Code   
FROM Fund a, Fund_Map b
WHERE a.Fund_Type = 'MMF' AND
a.Product_Type_Code = 'M' AND
a.Main_Fund_Code = b.Main_Fund_Code """
cur5 = con.cursor()
cur5.execute(q5)
cur5.connection.commit()

q6="CREATE TABLE ##All_MFs(Parent_Fund_Code Fund_Code_Class, Main_Fund_Code Fund_Code_Class, Unit_Distr_Cap Fractional_Dollar_Class NULL, Unit_Distr_Income Fractional_Dollar_Class NULL)"
cur6 = con.cursor()
cur6.execute(q6)
cur6.connection.commit()

q7="""INSERT INTO ##All_MFs
SELECT a.Parent_Fund_Code, a.Main_Fund_Code, Unit_Distr_Cap, Unit_Distr_Income 
FROM ##PF_Codes a, ##All_Distributions b WHERE 
a.Main_Fund_Code *= b.Main_Fund_Code AND
a.Parent_Fund_Code *= b.Parent_Fund_Code 
ORDER BY Parent_Fund_Code"""
cur7 = con.cursor()
cur7.execute(q7)
cur7.connection.commit()

q8 = "CREATE TABLE ##Pending_Purch(Fund_Code Fund_Code_Class)"
cur8 = con.cursor()
cur8.execute(q8)
cur8.connection.commit()


q9="""INSERT INTO ##Pending_Purch
SELECT DISTINCT a.Fund_Code 
FROM WO_Purch a, WO_Placement b, ##All_MFs c, Entry_Fund_Code d
WHERE a.Fund_Code = d.Fund_Code AND
d.Main_Fund_Code = c.Main_Fund_Code AND
a.WO_No = b.WO_No AND
b.WO_Type = 'P' AND
b.Proc_Status = 'P'"""
cur9 = con.cursor()
cur9.execute(q9)
cur9.connection.commit()


q10= "CREATE TABLE ##Temp_Output(Parent_Fund_Code Fund_Code_Class, Main_Fund_Code Fund_Code_Class, Fund_Name varchar(45), Unit_Distr_Cap Fractional_Dollar_Class NULL, Unit_Distr_Income Fractional_Dollar_Class NULL, Report varchar(2))"
cur10 = con.cursor()
cur10.execute(q10)
cur10.connection.commit()

q11="""INSERT INTO ##Temp_Output
SELECT DISTINCT a.Parent_Fund_Code, a.Main_Fund_Code, d.Fund_Name, a.Unit_Distr_Cap, a.Unit_Distr_Income, '' as Report 
FROM ##All_MFs a , Daily_Fund_Share_Balance b, ##Pending_Purch c, Fund d
WHERE a.Main_Fund_Code = b.Main_Fund_Code AND
a.Main_Fund_Code = d.Main_Fund_Code AND
b.Posting_Date = (SELECT Last_Posting_Date FROM System_Control) AND
((Units_NCI + Units_CI - Units_OSRED) > 0 OR a.Main_Fund_Code = c.Fund_Code)"""
cur11 = con.cursor()
cur11.execute(q11)
cur11.connection.commit()


q12="CREATE TABLE ##Out_PFCodes(Parent_Fund_Code Fund_Code_Class)"
cur12 = con.cursor()
cur12.execute(q12)
cur12.connection.commit()

q13="""INSERT INTO ##Out_PFCodes
SELECT DISTINCT Parent_Fund_Code 
FROM ##Temp_Output
WHERE Unit_Distr_Income IS NULL"""
cur13 = con.cursor()
cur13.execute(q13)
cur13.connection.commit()

q14 = """UPDATE ##Temp_Output SET Report = 'Y'"""
# SET Report = 'Y' WHERE Parent_Fund_Code IN (Select Parent_Fund_Code FROM ##Out_PFCodes)"""
cur14 = con.cursor()
cur14.execute(q14)
cur14.connection.commit()

q15="""SELECT a.Parent_Fund_Code, a.Main_Fund_Code, b.Fund_Name, Unit_Distr_Cap, Unit_Distr_Income, b.T_Fund_Flag, b.Corp_Trust_Code, b.Fund_Type
FROM ##Temp_Output a, Fund b
WHERE Report = 'Y' AND
a.Main_Fund_Code = b.Main_Fund_Code
group by a.Parent_Fund_Code, a.Main_Fund_Code, b.Fund_Name, Unit_Distr_Cap, Unit_Distr_Income, 
b.T_Fund_Flag, b.Corp_Trust_Code, b.Fund_Type
ORDER BY a.Parent_Fund_Code"""
cur15 = con.cursor()
cur15.execute(q15)





def print_row(row):
	row_string = ""
	remaining_in_parent = 9 - len(str(row[0]).rstrip('\n'))
	remaining_in_main = 9 - len(str(row[1]).rstrip('\n'))
	#calculate the number of remaining spaces in the Fund_Name cell by subtracting the length of the current row's Fund_Name from the maximum Fund_Name length, which is 41 as of 08/14/2018. 
	#Maximum Fund_Name length can be adjusted below later if Fund_Name lengths increase or decrease. The same applies to all other 'remaining_in' local variables
	remaining_in_name_cell = 41 - len(str(row[2].encode('utf-8').rstrip('\n')))
	if ('®' in str(row[2].encode('utf-8').rstrip('\n'))):
		remaining_in_name_cell += 1
	remaining_in_cap = 10 - len(str(row[3]).rstrip('\n'))
	remaining_in_income = 10 - len(str(row[4]).rstrip('\n'))
	remaining_in_tflag = 5 - len(str(row[5]))
	remaining_in_corp = 6 - len(str(row[6]))
	remaining_in_type = 10 - len(str(row[7]))
	row_string += str(row[0]).rstrip('\n') + (" " * remaining_in_parent) + "," + str(row[1]).rstrip('\n') + (" " * remaining_in_main) + "," + str(row[2].encode('utf-8').rstrip('\n')) + (" " * remaining_in_name_cell) + "," + str(row[3]).rstrip('\n') + (" " * remaining_in_cap) +  "," + \
	str(row[4]).rstrip('\n') + (" " * remaining_in_income) + ","  + str(row[5]).rstrip('\n') + (" " * remaining_in_tflag) +  "," + str(row[6]).rstrip('\n') + (" " * remaining_in_corp) + "," + \
	str(row[7]).rstrip('\n') + (" " * remaining_in_type)
	return row_string

rows = cur15.fetchall()
rows_written = 0
for row in rows:
	row = list(row)
	if row[0] == None:
		row[0] = "No Code"
	if row[1] == None:
		row[1] = "No Code"
	if row[2] == None:
		row[2] = "No Name"
	if row[3] == None:
		row[3] = "No Data"
	if row[4] == None:
		row[4] = "No Data"
	if row[5] == None:
		row[5] = "No Data"
	if row[6] == None:
		row[6] = "No Data"
	if row[7] == None:
		row[7] = "No Data"
	
	f.write(print_row(row))
	f.write("\n")
	rows_written += 1
	row = tuple(row)

if (rows_written == 0):
	f.write("\nNo Distributions for " + str(today_date.strftime('%m-%d-%y')))

f.close()
con.close()
