import logging

from pymongo import MongoClient

class Loan:

    conn = None

    def __init__(self):
        None

    def getLoansByDate(self, date):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            loans = db['loans']

            return loans.find({"$and": [{"initialDateOfAccrual": {'$gte': date}},{"status":"ACTIVE"}]})
        except Exception as exception: 
            logging.error("Error getloansToInitilize - %s." % str(exception))


    def getQtyLoansByDate(self, date):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            loans = db['loans']

            return loans.count_documents({"$and": [{"initialDateOfAccrual": {'$gte': date}},{"status":"ACTIVE"}]})
        
        except Exception as exception: 
            logging.error("Error getloansToInitilize - %s." % str(exception))

    
    def getQtyLoansByDateandOrgId(self, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            loans = db['loans']

            return loans.count_documents({"$and": [{"initialDateOfAccrual": {'$gte': date}}, {"organizationId":orgId},{"status":"ACTIVE"}]})
        
        except Exception as exception: 
            logging.error("Error getloansToInitilize - %s." % str(exception))

    def getQtyLoansByDateandOrgIdWithoutStatus(self, date, orgId):

        conn = None

        try:
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@10.0.1.198:27017/Customers?authSource=admin')
            #conn = MongoClient('mongodb://juno_loan:VEUSUdrLmCLd9pE6PveQY8ahgnNqL2f2@qa-juno-mongodb.cluster-c5ebb2jx7zvm.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')
            conn = MongoClient('mongodb://juno_loan:nHgSN5JfHDwz9yexpUNj2x5jED3UgCrC@prod-juno-mongodb.cluster-ro-ctza19neneum.us-east-1.docdb.amazonaws.com:27017/Loans?authSource=admin&retryWrites=false')

            db = conn['Loans']
            loans = db['loans']

            return loans.count_documents({"$and": [{"initialDateOfAccrual": {'$lt': date}}, {"organizationId":orgId}]})
        
        except Exception as exception: 
            logging.error("Error getloansToInitilize - %s." % str(exception))

