import logging

from pymongo import MongoClient

class Accrual:

    conn = None

    def __init__(self):
        None

    def getAccrualByLoanIdAndDateAndOrgIdList(self, loanId, date, orgId):

        conn = None

        try:
            conn = MongoClient('Completar con  el host')

            db = conn['Loans']
            accruals = db['accruals']
         
            return accruals.count_documents({"$and": [{"loanId": loanId}, {"date": date}, {"organizationId": orgId}]})

        except Exception as exception: 
            logging.error("Error getaccrualsByDatesAndOrgId - %s." % str(exception))


    def getAccrualsByDate(self, date):

        conn = None

        try:
            conn = MongoClient('Completar con  el host')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.find({"$and": [{"date": {'$gte': date}}]})
        except Exception as exception: 
            logging.error("Error getaccrualsToInitilize - %s." % str(exception))


    def getQtyAccrualsByDate(self, date):

        conn = None

        try:
            conn = MongoClient('Completar con  el host')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.count_documents({"$and": [{"date": {'$gte': date}}]})
        
        except Exception as exception: 
            logging.error("Error getaccrualsToInitilize - %s." % str(exception))

    def getQtyAccrualsByDateandOrgId(self, date, orgId):

        conn = None

        try:
            conn = MongoClient('Completar con  el host')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.count_documents({"$and": [{"date": {'$gte': date}}, {"organizationId":orgId}]})
        
        except Exception as exception: 
            logging.error("Error getaccrualsToInitilize - %s." % str(exception))


    def getAccrualsByDateAndOrgId(self, date, orgId):

        conn = None

        try:
            conn = MongoClient('Completar con  el host')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.find({"$and": [{"date": date}, {"organizationId": orgId}]})
        except Exception as exception: 
            logging.error("Error getaccrualsByDatesAndOrgId - %s." % str(exception))