from datetime import datetime, timedelta
from collections import OrderedDict
import calendar


class Utils:

    @staticmethod
    def generate_months_between_date_range(min_date, max_date):
        # Convert both dates to Strings
        import datetime as dated
        min_dt = min_date.strftime("%Y-%m-%d") if isinstance(min_date, (datetime, dated.date)) else min_date
        max_dt = max_date.strftime("%Y-%m-%d") if isinstance(max_date, (datetime, dated.date)) else max_date
        dates = [min_dt, max_dt]
        # print(dates)
        start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in dates]
        return [OrderedDict(((start + timedelta(_)).strftime(r"%Y-%m"), None) for _ in range((end - start).days)).keys()]

    @staticmethod
    def last_day_of_month(dt):
        import datetime as dated
        if isinstance(dt, dated.date):
            return calendar.monthrange(dt.year, dt.month)[1]
        else:
            date_format = datetime.strptime(dt, "%Y-%m-%d")
            return calendar.monthrange(date_format.year, date_format.month)[1]

    @staticmethod
    def action_type(var: int):
        # 2 => repayment, 4 => waive_interest,5 => repayment_at_disbursement 9 => waive_charges
        if var in (2, 4, 5, 9):
            return -1

    @staticmethod
    def get_loan_status(var: int):
        switcher = {
            100: "SUBMITTED",
            200: "APPROVED",
            300: "ACTIVE",
            400: "WITHDRAWN",
            500: "REJECTED",
            600: "CLOSED",
            601: "CANCELED",
            700: "OVERPAID",
        }
        return switcher.get(var, "UNKNOWN")

    def classify_dpd(self, band):
        pass