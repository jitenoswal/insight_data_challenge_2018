
# Table of Contents
1. [Introduction](README.md#introduction)
2. [Approach](README.md#approach)
3. [Approximate Code](README.md#approximate-Code)
4. [Test Written](README.md#test-written)

# Introduction

This is my approach to solving Insight Data Challenge where we need to find transaction from repeat donors to committee/recipient based on Federcal Election Commission Data published on their public website.

# Approach

First thing, I think was important after doing data-validation was to figure out logic to find out Unique Repeat Donors. 
Next, was to find a way list out transaction for each committee/recipient who received Unique Repeat Donors. 
Also, figuring our alorigthm for calculating sum of donations at a given point received and xth percentile was the next 2 important things

Data-Validation Done: 


# Approximate Code

1. Data-Validation of all sorts 
2. Add new column to represent: Uniqueness of Donor was indirectly given to be used as a combination of Donor NAME & ZIP_CODE. 
3. Add new column to represent: Uniqueness of Recipient was indirectly given to be used as a combination of CMTE_ID & ZIP_CODE & TRANSACTION_DT(Year).
4. Use pandas duplicate api to find all repeat UNIQ_DONOR
5. For row in data_frame
	1. Check if Recipient_Id_ZC_Year exists in Recipient_HM
        1. Write “Recipient_Id | ZipCode | Year | perf30(row_amt, Recipient_HM_amt) | sum(row_amt + Recipient_HM_amt) | Recipient_HM_count ++”
    2. Else
        1. Add Recipient_Id_ZC_Year, Recipient_Id, ZipCode, row_amt, 1 
        2. Write “Recipient_Id | ZipCode | Year | row_amt | row_amt | 1”


# Test Written

1. Covered basic setup test for missing input files and output filepath. 
2. Column constraints are enforced appropriately. 
3. 7 types of test inputs from 10kb (example provided by the team) and large files 1.25 Gigs from the Federal Commission website.

