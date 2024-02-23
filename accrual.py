import logging

from pymongo import MongoClient

class Accrual:

    conn = None

    def __init__(self):
        None

    def getAccrualByLoanIdAndDateAndOrgIdList(self, loanId, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            accruals = db['accruals']
         
            return accruals.count_documents({"$and": [{"loanId": loanId}, {"date": date}, {"organizationId": orgId}]})

        except Exception as exception: 
            logging.error("Error getaccrualsByDatesAndOrgId - %s." % str(exception))


    def getAccrualsByDate(self, date):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.find({"$and": [{"date": {'$gte': date}}]})
        except Exception as exception: 
            logging.error("Error getaccrualsToInitilize - %s." % str(exception))


    def getQtyAccrualsByDate(self, date):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.count_documents({"$and": [{"date": {'$gte': date}}]})
        
        except Exception as exception: 
            logging.error("Error getaccrualsToInitilize - %s." % str(exception))

    def getQtyAccrualsByDateandOrgId(self, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.count_documents({"$and": [{"date": {'$gte': date}}, {"organizationId":orgId}]})
        
        except Exception as exception: 
            logging.error("Error getaccrualsToInitilize - %s." % str(exception))


    def getAccrualsByDateAndOrgId(self, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            accruals = db['accruals']

            return accruals.find({"$and": [{"date": date}, {"organizationId": orgId}]})
        except Exception as exception: 
            logging.error("Error getaccrualsByDatesAndOrgId - %s." % str(exception))