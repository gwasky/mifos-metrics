import os, logging
from sqlalchemy import engine, Column, Integer, Float, Date, Boolean
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
from utils import Utils
import pandas as pd
from pprint import pprint

logging.basicConfig(format='[%(asctime)s] - [%(levelname)-8s] - [%(name)s:%(lineno)d ] - %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG,
                    filename=os.path.join(os.path.dirname(__file__), 'logs', 'pdp_new.log'))
logger = logging.getLogger("main")

# connection
db = "test"
engine = engine.create_engine("mysql+pymysql://%s:%s@%s/%s" % (os.getenv('PROD_DB_USR'), os.getenv('PROD_DB_PASS'), os.getenv('PROD_DB_URL'), db))

# Map entities
Base = declarative_base()


class LoanModel(Base):
    __tablename__ = "m_loan"
    id = Column(Integer, primary_key=True)
    loan_status_id = Column(Integer)
    disbursedon_date = Column(Date)
    principal_amount = Column(Float)
    approved_principal = Column(Float)
    interest_charged_derived = Column(Float)
    fee_charges_charged_derived = Column(Float)
    principal_disbursed_derived = Column(Float)
    total_expected_repayment_derived = Column(Float)


class LoanScheduleModel(Base):
    __tablename__ = "m_loan_repayment_schedule"
    id = Column(Integer, primary_key=True)
    loan_id = Column(Integer)
    fromdate = Column(Date)
    duedate = Column(Date)
    installment = Column(Integer)
    completed_derived = Column(mysql.BIT(1))
    obligations_met_on_date = Column(Date)
    principal_amount = Column(Float)
    interest_amount = Column(Float)
    fee_charges_amount = Column(Float)
    penalty_charges_amount = Column(Float)


class TransactionModel(Base):
    __tablename__ = "m_loan_transaction"
    id = Column(Integer, primary_key=True)
    loan_id = Column(Integer)
    is_reversed = Column(Boolean)
    transaction_type_enum = Column(Integer)
    transaction_date = Column(Date)
    amount = Column(Float)
    principal_portion_derived = Column(Float)
    interest_portion_derived = Column(Float)
    fee_charges_portion_derived = Column(Float)
    penalty_charges_portion_derived = Column(Float)
    overpayment_portion_derived = Column(Float)
    outstanding_loan_balance_derived = Column(Float)

if __name__ == '__main__':

    logger.info("__main__")
    # Query
    factory = sessionmaker(bind=engine)
    session = factory()
    # q_loan = session.query(LoanModel).filter(LoanModel.loan_status_id.notin_([100, 500, 200]), LoanModel.id.in_([9, 86, 123]))
    q_loan = session.query(LoanModel).filter(LoanModel.loan_status_id.notin_([100, 500, 200]))
    logger.info("Number of loans - %s" % q_loan.count())
    data = dict()

    for loan in q_loan:
        # logger.info("loan_id - %s - %s - %s" % (loan.id, loan.loan_status_id, loan.disbursedon_date))
        if loan.id in [166]:
            # if True:
            loan_id: int = loan.id
            data.setdefault(str(loan_id), {})
            schedule_map: dict = dict()
            q_schedule = session.query(LoanScheduleModel).filter(LoanScheduleModel.loan_id == loan.id).order_by(LoanScheduleModel.installment)
            q_transaction = session.query(TransactionModel).filter(TransactionModel.loan_id == loan.id, ).order_by(TransactionModel.id)
            min_date = sorted(q_schedule, key=lambda x: x.duedate)[0].duedate
            months: list = list(Utils.generate_months_between_date_range(min_date=min_date, max_date=datetime.now())[0])
            for schedule in q_schedule:
                expected_repayment = (schedule.principal_amount or 0) + \
                                     (schedule.interest_amount or 0) + \
                                     (schedule.fee_charges_amount or 0) + \
                                     (schedule.penalty_charges_amount or 0)
                # logger.info("loan_id - %s - %s - %s - %s - %s" % (last_date_of_month, loan.id, schedule.duedate, last_obligations_met_on_date, expected_repayment))
                logger.info("loan_id - %s - %s - %s - %s" % (loan.id, schedule.duedate, schedule.obligations_met_on_date, expected_repayment))

                schedule_map[schedule.duedate] = expected_repayment

            cummulative_balance = dict()
            for month in months:
                last_month_day: int = Utils.last_day_of_month(month + "-01")
                last_date_of_month_str: str = month + '-' + str(last_month_day)
                last_date_of_month: date = datetime.strptime(last_date_of_month_str, "%Y-%m-%d").date()
                for key in schedule_map:
                    if key <= last_date_of_month:
                        if last_date_of_month in cummulative_balance.keys():
                            cummulative_balance[last_date_of_month] += schedule_map[key]
                        else:
                            cummulative_balance.setdefault(last_date_of_month, schedule_map[key])

                first_date_of_month: datetime = datetime.strptime(month + '-01', "%Y-%m-%d")

            pprint(cummulative_balance)

            records = []
            for l_record in data:
                for month in data[l_record]:
                    x = [l_record, month, data[l_record][month]['overdue_days']]
                    records.append(x)
                    # Write to CSV
            df = pd.DataFrame(records)
            df.columns = ['loan_id', 'report_date', 'dpd']
            df.to_csv('data/dpd_new.csv')
