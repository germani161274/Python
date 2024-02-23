import logging

from pymongo import MongoClient

class Balance:

    conn = None

    def __init__(self):
        None

    def getBalanceByLoanIdAndDateAndOrgIdList(self, loanId, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            balances = db['balances']
         
            return balances.count_documents({"$and": [{"loanId": loanId}, {"date": date}, {"organizationId": orgId}]})

        except Exception as exception: 
            logging.error("Error getBalancesByDatesAndOrgId - %s." % str(exception))


    def getBalancesByDate(self, date):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            balances = db['balances']

            return balances.find({"$and": [{"date": {'$gte': date}}]})
        except Exception as exception: 
            logging.error("Error getBalancesToInitilize - %s." % str(exception))


    def getQtyBalancesByDate(self, date):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            balances = db['balances']

            return balances.count_documents({"$and": [{"date": {'$gte': date}}]})
        
        except Exception as exception: 
            logging.error("Error getBalancesToInitilize - %s." % str(exception))

    def getQtyBalancesByDateandOrgId(self, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            balances = db['balances']

            return balances.count_documents({"$and": [{"date": {'$gte': date}}, {"organizationId":orgId}]})
        
        except Exception as exception: 
            logging.error("Error getBalancesToInitilize - %s." % str(exception))


    def getBalancesByDateAndOrgId(self, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            balances = db['balances']

            return balances.find({"$and": [{"date": date}, {"organizationId": orgId}]})
        except Exception as exception: 
            logging.error("Error getBalancesByDatesAndOrgId - %s." % str(exception))