
# Table of Contents
1. [Introduction](README.md#introduction)
2. [Approach](README.md#approach)
3. [Approximate Code](README.md#approximate-Code)
4. [Test Written](README.md#test-written)

# Introduction

This is my approach to solving Insight Data Challenge where we need to find transaction from repeat donors to committee/recipient based on Federcal Election Commission Data published on their public website.

Imports required: 

1. import sys
2. import pandas as pd
3. import numpy as np
4. import os, errno
5. import logging


# Approach

First thing, I think was important after doing data-validation was to figure out logic to find out Unique Repeat Donors. 
Next, was to find a way list out transaction for each committee/recipient who received Unique Repeat Donors. 
Also, figuring our alorigthm for calculating sum of donations at a given point received and xth percentile was the next 2 important things

# Approximate Code (I can go over the code again if anyone has doubts)

1. Data-Validation of all sorts 
2. Add new column to represent: Uniqueness of Donor was indirectly given to be used as a combination of Donor NAME & ZIP_CODE. 
3. Add new column to represent: Uniqueness of Recipient was indirectly given to be used as a combination of CMTE_ID & ZIP_CODE & TRANSACTION_DT(Year).
4. Use pandas duplicate api to find all repeat UNIQ_DONOR
5. For row in data_frame
	1. Check if Recipient_Id_ZC_Year exists in Recipient_dict
		1. Write “Recipient_Id | ZipCode | Year | perf30(row_amt, Recipient_dict_amt) | sum(row_amt + Recipient_dict_amt) | Recipient_HM_count ++”
		2. Update Recipient_dict with new record
	2. Else
		1. Add Recipient_Id_ZC_Year, Recipient_Id, ZipCode, row_amt, 1 
		2. Write “Recipient_Id | ZipCode | Year | row_amt | row_amt | 1”


# Test Written

1. Test_1: Example 1 provided in challenge (Happy Case)
2. Test_2: Example 2 provided in challenge (Out of Order Record Handling by skipping it completely)
3. Test_3: And updated version of Example 1
4. Test_4: Real World data downloaded from Federal Election Commission for latest year 2017-2018 (1.25 Gb file has ~7+ million records): This takes about 4 minutes after many optimizations but given some more time I could further optimize it by either implementing a cython method or trying rolling_sum tricks of pandas.
5. Test_5: 80% of the records in this tests don't satisfy our column constraints criteria. Rest 20% records are non repeat donor so output is actually a blank file. 
6. Test_6: All 100% of the records in this tests don't satisfy our column constraints criteria eventually resulting into blank output file. 
7. Test_7: Partially chunk of Test_4 Federal Election Commission file to validate output.  



