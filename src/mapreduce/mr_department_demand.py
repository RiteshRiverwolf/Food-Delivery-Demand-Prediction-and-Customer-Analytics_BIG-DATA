"""MapReduce job 3: demand and customer loyalty per department (map-side join).

Input:  order_products__prior.csv
Side files: --products products.csv, --departments departments.csv
Output: CSV lines "department,items_ordered,reorder_rate"

Each mapper loads the small product -> department lookup into memory, so the
32M-row fact table is joined without a shuffle of the lookup tables.
"""
import csv

from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol


class MRDepartmentDemand(MRJob):
    OUTPUT_PROTOCOL = TextValueProtocol

    def configure_args(self):
        super().configure_args()
        self.add_file_arg("--products", help="path to products.csv")
        self.add_file_arg("--departments", help="path to departments.csv")

    def mapper_init(self):
        with open(self.options.departments, newline="", encoding="utf-8") as fh:
            dept_names = {r["department_id"]: r["department"] for r in csv.DictReader(fh)}
        with open(self.options.products, newline="", encoding="utf-8") as fh:
            self.dept_of = {r["product_id"]: dept_names[r["department_id"]] for r in csv.DictReader(fh)}

    def mapper(self, _, line):
        if line.startswith("order_id"):
            return
        # columns: order_id,product_id,add_to_cart_order,reordered
        f = line.split(",")
        yield self.dept_of.get(f[1], "unknown"), (1, int(f[3]))

    def combiner(self, dept, pairs):
        items = reorders = 0
        for i, r in pairs:
            items += i
            reorders += r
        yield dept, (items, reorders)

    def reducer(self, dept, pairs):
        items = reorders = 0
        for i, r in pairs:
            items += i
            reorders += r
        yield None, f"{dept},{items},{reorders / items:.4f}"


if __name__ == "__main__":
    MRDepartmentDemand.run()
