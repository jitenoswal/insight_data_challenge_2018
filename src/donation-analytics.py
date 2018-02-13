#!/usr/bin/python

import sys
import pandas as pd
import numpy as np
import os, errno
import logging

#
# Reading args passed in command line
#
input_filepath = sys.argv[1]
perc_filepath = sys.argv[2]
output_filepath = sys.argv[3]

Col_Defs = ["CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI", "IMAGE_NUM", "TRANSACTION_TP", "ENTITY_TP", "NAME", "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION", "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "TRAN_ID", "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID"]

# Columns we are more concerned as mentioned for our challenge are: 
# [0] CMTE_ID: C00629618			[7] NAME: PEREZ, JOHN A 	[10] ZIP_CODE: 90017
# [13] TRANSACTION_DT: 01032017 	[14] TRANSACTION_AMT: 40 	[15] OTHER_ID: H6CA34245
Use_Cols = ["CMTE_ID","NAME","ZIP_CODE","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID"]

#
# Setup logging schema
#
def setup_logger():
	# define a Handler which writes INFO messages or higher to the sys.stderr
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
	return logging

#
# Counting lines in file
#
def count_lines_in_file(filepath):
	num_lines = sum(1 for line in open(filepath))
	return num_lines

#
# Function to calculate percentile using nearest-rank method
#
def calc_percentile (perc, ordered_list, len):
    ind = int(round((float(perc)/100)*len))
    if ind != 0:
    	ind = ind - 1
    return ordered_list[ind]

#
# Function to remove output file: repeat_donors.txt file in case it exists
#
def validating_setup(input_filepath,output_filepath,perc_filepath):
	logging.info('Validating Setup')
	try:
		os.path.exists(input_filepath)
	except OSError,IOError:
		logging.error('Input file path has some issues. Please check!')
		quit()
	try:
		os.path.exists(perc_filepath)
	except OSError, IOError:
		logging.error('Percentile file path has some issues. Please check!')
		quit()
	try:
		os.remove(output_filepath)
	except OSError:
		pass
	logging.info('Done validating setup')


#
# Function to read percentile value from percentile.txt to be used for calculation
#
def read_perc_file(perc_filepath):
	logging.info('Reading Percentile from file')
	if count_lines_in_file(perc_filepath) == 1:
		with open(perc_filepath) as fp:
			line = fp.readline()
		try:
			val = int(line.strip())
		except :
			logging.error('Percentile file has non integer value in it. Please check and pass in correctly formatted file!')	
			exit()
	else:
		logging.error('Percentile file has more than 1 lines so is incorrectly created. Please check and pass in correctly format file!')
		exit()
	logging.info('Done reading Percentile from file')
	return val

#
# Function to check and validate data in each column:
# CMTE_ID, NAME, ZIP_CODE, TRANSACTION_DT, TRANSACTION_AMT: column shouldn't have null values
# OTHER_ID: column so only considering those which are empty
# ZIP_CODE, TRANSACTION_DT, TRANSACTION_AMT: column should only have numeric charaters
#
def check_and_update_df_on_col_value_constraints(primary_df):
	try:
		logging.info('[Processing] Let\'s check and fix missing/empty values. Also, validating value based on value constraints :- ')
	
		primary_df = primary_df[primary_df['CMTE_ID'].notnull() & primary_df['NAME'].notnull() & primary_df['TRANSACTION_DT'].notnull() & primary_df['TRANSACTION_AMT'].notnull() & primary_df['ZIP_CODE'].notnull() & primary_df['OTHER_ID'].isnull()]		
		primary_df = primary_df[primary_df['ZIP_CODE'].str.isnumeric()]
		primary_df = primary_df[primary_df['TRANSACTION_AMT'].str.isnumeric()]
		primary_df = primary_df[primary_df['TRANSACTION_DT'].str.isnumeric()]
		clean_df = primary_df[primary_df['TRANSACTION_DT'].notnull() & primary_df['TRANSACTION_AMT'].notnull() & primary_df['ZIP_CODE'].notnull()]

		clean_df['TRANSACTION_AMT'] = pd.to_numeric(clean_df['TRANSACTION_AMT'], errors='coerce')
	
		clean_df['TRANSACTION_DT'] = clean_df['TRANSACTION_DT'].str[-4:]					# TRANSACTION_DT: column so extracting only last 4 digits representing year
	#	clean_df['TRANSACTION_AMT'] = clean_df['TRANSACTION_AMT'].astype(int)				# AMT: column as it has to be an integer
		clean_df['ZIP_CODE'] = clean_df['ZIP_CODE'].str[0:5]								# ZIP: column so extracting only first 5 digits including 0
		clean_df = clean_df[clean_df['TRANSACTION_DT'].notnull() & clean_df['TRANSACTION_AMT'].notnull() & clean_df['ZIP_CODE'].notnull()]
		clean_df.is_copy = False

		logging.info('[Done] We now have conformed data ready. ')
	except :
		with open(output_filepath, 'a') as the_file:
			logging.info('There\'s no data in this file which passes are column rules/constraints')	
		exit()

	return clean_df


#
# Massaging data which straightens it up for future usage.
# NAME: replacing ' ' with '_'  
# UNIQ_DONOR: concat NAME + ZIPCODE identifier
# RECIPIENT_ID_ZC_YEAR: concat CMTE_ID + ZIPCODE + YEAR identifier
#
def add_magic_uniq_identifier_col(clean_df):
	try:
		logging.info('[Processing] Adding some magic columns now :- ')
		clean_df['NAME'] = clean_df['NAME'].str.replace(' ', '_')						# NAME: replacing ' ' with '_' 
		uniq_donor = clean_df['NAME'] + '_' + clean_df['ZIP_CODE']						# UNIQ_DONOR: concat NAME+ZIPCODE identifier 
		clean_df = pd.concat([clean_df, uniq_donor.rename('UNIQ_DONOR')], axis=1)	
		Recipient_Id_ZC_Year = clean_df['CMTE_ID'] + '_' + clean_df['ZIP_CODE'] + '_' + clean_df['TRANSACTION_DT'] # RECIPIENT_ID_ZC_YEAR: concat CMTE_ID+ZIPCODE+YEAR identifier
		
		# Logic to take care of out of order records. current >= previous
		# Created a shifted df to as filter for only selecting records which follow this order
		#
		clean_df['TRANSACTION_DT'] = pd.to_numeric(clean_df['TRANSACTION_DT'], errors='coerce', downcast='integer')
		shifted_trans_dt = clean_df.TRANSACTION_DT.shift(1)
		shifted_trans_dt.iloc[0] = -99
		clean_df = clean_df.loc[ clean_df['TRANSACTION_DT'] >= shifted_trans_dt]

		clean_df = pd.concat([clean_df, Recipient_Id_ZC_Year.rename('RECIPIENT_ID_ZC_YEAR')], axis=1)
		clean_df = clean_df[clean_df['TRANSACTION_DT'].notnull()]
		logging.info('[Done] Magic columns added. ')

		logging.info('[Processing] Process for figuring out out all repeat donor')

		clean_df['UNIQ_DONOR'] = clean_df['UNIQ_DONOR'].duplicated()
		logging.info('Length of UNIQ_DONOR DF: '+ str(len(clean_df.loc[clean_df['UNIQ_DONOR']==1])))
		clean_df = clean_df.loc[clean_df['UNIQ_DONOR']==1]

		logging.info('[Done] Process for figuring out out all repeat donor')
	
	except :
		with open(output_filepath, 'a') as the_file:
			logging.info('There\'s no data in this file which passes are column rules/constraints')	
			exit()
	
	return clean_df

#
# Read CSV Input file 
#
def read_input_file_sel_col(input_filepath):
	logging.info('[Processing] Let\'s read input file :- ')
	big_df = pd.read_csv(input_filepath, sep='|',engine='c',dtype='str', names=Col_Defs, usecols=Use_Cols)		#Use_Cols = ["CMTE_ID","NAME","ZIP_CODE","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID"]
	logging.info('[Done] Done reading input file. ')
	return big_df


logging = setup_logger()
logging.info('Running Main ' + input_filepath)


print('\n ----------- Running main ----------- ')

validating_setup(input_filepath,output_filepath,perc_filepath)

perc = read_perc_file(perc_filepath)

logging.info('Input File Path --> ' + input_filepath)
logging.info('Percentile File Path --> ' + perc_filepath)
logging.info('Percentile to be calculated as per value in Percentile File --> ' + str(perc))
logging.info('Output File Path --> ' + output_filepath)

primary_df = read_input_file_sel_col(input_filepath)
logging.info('Length of Input DF: '+str(len(primary_df)))

clean_df = check_and_update_df_on_col_value_constraints(primary_df)
logging.info('Length of Clean DF: '+str(len(clean_df)))
print(clean_df.head(5))

clean_df = add_magic_uniq_identifier_col(clean_df)
logging.info('Length of final Clean DF: '+str(len(clean_df)))
print(clean_df.head(5))


print ('\n -- Data cleaned and ready for processing ---\n')

if len(clean_df) == 0:
	with open(output_filepath, 'a') as the_file:
		logging.info('It\'s seems there\'s no reocrd in this file which passes our validation/constraints\n')
	exit()

logging.info('Let\'s start doing magic ----\n')

donors_uniq_dict = {}							# donors_uniq_dict: Dictionary to represent DonorName_ZipCode's unique combination
Recipient_Id_ZC_Year_dict = {}					# Recipient_Id_ZC_Year_dict: Dictionary to represent RecipientID_ZipCode_Year's unique combination

# index = 1
# row 1: CMTE_ID
# row 2: NAME
# row 3: ZIP_CODE (5 digit)
# row 4: TRANSACTION_DT (4 digit)
# row 5: TRANSACTION_AMT 
# row 6: Nan 
# row 7: UNIQ_DONOR (DONOR_NAME + ZIP_CODE)
# row 8: RECIPIENT_ID_ZC_YEAR (RECIPIENT_ID + ZIP_CODE + TRANSACTION_DT)

logging.info('Length of our Input File Dataframe: ' + str(len(primary_df)))
logging.info('Length of our Final Clean Dataframe: ' + str(len(clean_df)))

logging.info('[Processing] Start processing data to generate output file ----\n')

#for row in clean_df.itertuples():
for row_id, row in enumerate(clean_df.values):
	index = 0
	uniq_donor = row[index+6]
	# print("row 0: " + str(row[index+0]))
	# print("row 1: " + str(row[index+1]))
	# print("row 2: " + str(row[index+2]))
	# print("row 3: " + str(row[index+3]))
	# print("row 4: " + str(row[index+4]))
	# print("row 5: " + str(row[index+5]))
	# print("row 6: " + str(row[index+6]))
	# print("row 7: " + str(row[index+7]))

	Recipient_Id_ZC_Year = row[index+7]
	if Recipient_Id_ZC_Year in Recipient_Id_ZC_Year_dict:
#		logging.info('Recipient_Id_ZC_Year exists in dictionary: ' + str(Recipient_Id_ZC_Year))
#		logging.info('Update --> dictionary')
		Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][4] = Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][4] + 1
		Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][5].append(row[index+4])
		Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][3] = int(round((float(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][3] + row[index+4]))))
#		logging.info('Write --> Output file')
		with open(output_filepath, 'a') as the_file:
			the_file.write(str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][0]) + '|'+ str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][1]) + '|'+ str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][2])+ '|'+str(calc_percentile(perc, Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][5], Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][4]))+ '|'+str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][3]) + '|'+str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][4]) + '\n')
	else:
#		logging.info('Recipient_Id_ZC_Year doesnt exists in dictionary')
#		logging.info('Add to dictionary: ' + str(Recipient_Id_ZC_Year))
		Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year] = [row[index], row[index+2], int(row[index+3]), int(row[index+4]), 1, [row[index+4]]] 
#		logging.info('Write --> Output file')
		with open(output_filepath, 'a') as the_file:
			the_file.write(str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][0]) + '|'+ str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][1]) + '|'+ str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][2]) + '|'+str(calc_percentile(perc, Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][5], Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][4])) +'|' +str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][3]) + '|'+str(Recipient_Id_ZC_Year_dict [Recipient_Id_ZC_Year][4]) + '\n')
		
logging.info('[Done] Done processing data to generate output file ----\n')

logging.info('---- Checkout the stored variables ---- ')
#logging.debug('Donors_uniq_dict: ' + str(donors_uniq_dict))
#logging.debug('Recipient_Id_ZC_Year_dict: ' + str(Recipient_Id_ZC_Year_dict))

logging.info('Output File Path ----> ' + output_filepath)

print('\n ------------- Main ended ---------------- \n\n')
