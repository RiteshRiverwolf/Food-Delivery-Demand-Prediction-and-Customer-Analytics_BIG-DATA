"""MapReduce job 2: top products by demand, with reorder rate (two-step job).

Input:  order_products__prior.csv
Side file: --products products.csv (map-side lookup for product names)
Output: CSV lines "rank,product_id,product_name,times_ordered,reorder_rate"

Step 1 counts orders and reorders per product (with a combiner).
Step 2 sends every product to one reducer, which keeps the top N.
"""
import csv
import heapq

from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol
from mrjob.step import MRStep


class MRTopProducts(MRJob):
    OUTPUT_PROTOCOL = TextValueProtocol

    def configure_args(self):
        super().configure_args()
        self.add_file_arg("--products", help="path to products.csv")
        self.add_passthru_arg("--top", type=int, default=25)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count, combiner=self.combine_count, reducer=self.reduce_count),
            MRStep(reducer_init=self.load_products, reducer=self.reduce_top),
        ]

    def mapper_count(self, _, line):
        if line.startswith("order_id"):
            return
        # columns: order_id,product_id,add_to_cart_order,reordered
        f = line.split(",")
        yield f[1], (1, int(f[3]))

    def combine_count(self, product_id, pairs):
        orders = reorders = 0
        for o, r in pairs:
            orders += o
            reorders += r
        yield product_id, (orders, reorders)

    def reduce_count(self, product_id, pairs):
        orders = reorders = 0
        for o, r in pairs:
            orders += o
            reorders += r
        yield None, (orders, reorders, product_id)

    def load_products(self):
        with open(self.options.products, newline="", encoding="utf-8") as fh:
            self.names = {row["product_id"]: row["product_name"] for row in csv.DictReader(fh)}

    def reduce_top(self, _, stats):
        top = heapq.nlargest(self.options.top, stats)
        for rank, (orders, reorders, pid) in enumerate(top, 1):
            name = self.names.get(pid, "unknown").replace('"', "'")
            yield None, f'{rank},{pid},"{name}",{orders},{reorders / orders:.4f}'


if __name__ == "__main__":
    MRTopProducts.run()
