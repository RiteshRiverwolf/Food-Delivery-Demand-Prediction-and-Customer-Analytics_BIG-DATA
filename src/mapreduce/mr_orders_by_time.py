"""MapReduce job 1: order demand by day of week and hour of day.

Input:  orders.csv
Output: CSV lines "order_dow,order_hour_of_day,orders"

Answers: when do customers place orders? (the demand heatmap that drives rider scheduling)
"""
from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol


class MROrdersByTime(MRJob):
    OUTPUT_PROTOCOL = TextValueProtocol

    def mapper(self, _, line):
        if line.startswith("order_id"):
            return
        f = line.split(",")
        # columns: order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order
        yield (int(f[4]), int(f[5])), 1

    def combiner(self, slot, counts):
        yield slot, sum(counts)

    def reducer(self, slot, counts):
        dow, hour = slot
        yield None, f"{dow},{hour},{sum(counts)}"


if __name__ == "__main__":
    MROrdersByTime.run()
