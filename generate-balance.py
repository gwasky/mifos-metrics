"""
    ALGORITHM
    - get loans List[Loan]
        - for each loan
            - get min due date from the loan schedule -> Datetime
            - Generate Months between minimum date and current date -> String
            - for each month
                - get the schedules with due date <= last_date_of_current_month -> List<ScheduleModel>
                   - for each schedule
                        - keep increamenting the expected_fee_repayments -> Float
                - get the repayments transactions made <= last_date_of_current_month -> List<TransactionModel>
                    - for each transaction
                        - keep increamenting the actual_repayment -> Float
                balance = expected_fee_repayments + loan_balance -> Float | this excludes any interest that has not yet been incured
                balance = balance - actual_repayment -> Float
"""

import os, logging
from sqlalchemy import engine, Column, String, Integer, Float, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
from utils import Utils
import pandas as pd
from pprint import pprint

logging.basicConfig(format='[%(asctime)s] - [%(levelname)-8s] - [%(name)s:%(lineno)d ] - %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG,
                    filename=os.path.join(os.path.dirname(__file__), 'logs', 'logs-interest.log'))
logger = logging.getLogger("main")

# connection
db = "mifostenant-FikiaFinance"
engine = engine.create_engine("mysql+pymysql://%s:%s@%s/%s" % (os.getenv('PROD_DB_USR'), os.getenv('PROD_DB_PASS'), os.getenv('PROD_DB_URL'), db))

INCLUDE_INTEREST = True

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
    logger.info("main")
    # Query
    factory = sessionmaker(bind=engine)
    session = factory()
    # q_loan = session.query(LoanModel).filter(LoanModel.loan_status_id.notin_([100, 500, 200]), LoanModel.id.in_([9, 10, 11]))
    q_loan = session.query(LoanModel).filter(LoanModel.loan_status_id.notin_([100, 500, 200]))
    logger.info("Number of loans - %s" % q_loan.count())
    data = dict()

    for loan in q_loan:

        if True:
            loan_id: int = loan.id
            data.setdefault(str(loan_id), {})

            # get min date
            q_schedule = session.query(LoanScheduleModel).filter(LoanScheduleModel.loan_id == loan.id, LoanScheduleModel.duedate <= date(2022, 7, 31)).order_by(
                LoanScheduleModel.installment)
            # pprint(schedule_map)
            min_date = sorted(q_schedule, key=lambda x: x.duedate)[0].duedate
            months: list = list(Utils.generate_months_between_date_range(min_date=min_date, max_date=datetime.now())[0])

            for month in months:
                t_expected_fee_repayments = 0
                t_actual_repayment = 0
                last_month_day: int = Utils.last_day_of_month(month + "-01")
                last_date_of_month_str: str = month + '-' + str(last_month_day)
                last_date_of_month: datetime = datetime.strptime(last_date_of_month_str, "%Y-%m-%d")

                q_schedule = session.query(LoanScheduleModel) \
                    .filter(LoanScheduleModel.loan_id == loan.id, LoanScheduleModel.duedate <= last_date_of_month) \
                    .order_by(LoanScheduleModel.installment)

                for schedule in q_schedule:
                    expected_fee_repayments = (schedule.interest_amount or 0) + \
                                              (schedule.fee_charges_amount or 0) + \
                                              (schedule.penalty_charges_amount or 0)
                    logger.info("loan_id - %s - %s - %s" % (loan.id, schedule.duedate, expected_fee_repayments))
                    t_expected_fee_repayments += expected_fee_repayments

                q_transactions = session.query(TransactionModel).filter(
                    TransactionModel.loan_id == loan.id,
                    TransactionModel.transaction_date <= last_date_of_month,
                    TransactionModel.is_reversed.is_(False),
                    TransactionModel.transaction_type_enum.in_([2]))
                for transaction in q_transactions:
                    actual_repayment = (transaction.principal_portion_derived or 0) + \
                                       (transaction.interest_portion_derived or 0) + \
                                       (transaction.fee_charges_portion_derived or 0) + \
                                       (transaction.penalty_charges_portion_derived or 0)
                    logger.info("transactions - %s - %s - %s - %s - %s" % (loan.id,
                                                                           transaction.id,
                                                                           transaction.transaction_date,
                                                                           transaction.transaction_type_enum,
                                                                           actual_repayment))
                    t_actual_repayment += actual_repayment

                logger.info("payments - %s - %s - %s - %s" % (loan.id,
                                                              month,
                                                              t_expected_fee_repayments,
                                                              t_actual_repayment
                                                              ))

                balance = t_expected_fee_repayments + loan.principal_amount

                balance = balance - t_actual_repayment

                data[str(loan_id)][last_date_of_month_str] = balance

    pprint(data)
    records = []
    for loan_id in data:
        for report_date in data[loan_id]:
            x = [loan_id, report_date, data[loan_id][report_date]]
            records.append(x)
    df = pd.DataFrame(records)
    df.columns = ['loan_id', 'report_date', 'balance']
    df.to_csv('data/principal_balance.csv')

    session.close()
