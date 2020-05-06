
from prettytable import PrettyTable
import json

with open('query3.json') as f:
    data = json.load(f)

table = PrettyTable()
table.field_names = data['select']

for key, value in {('Bloom',): {'1_sum_quant': 47080, '1_count_quant': 18, '1_avg_quant': 2615.5555555555557, '2_sum_quant': 72415, '2_count_quant': 29}, ('Sam',): {'1_sum_quant': 50996, '1_count_quant': 21, '1_avg_quant': 2428.3809523809523, '2_sum_quant': 67227, '2_count_quant': 26}, ('Helen',): {'1_sum_quant': 68269, '1_count_quant': 29, '1_avg_quant': 2354.103448275862, '2_sum_quant': 70706, '2_count_quant': 30}, ('Emily',): {'1_sum_quant': 62330, '1_count_quant': 24, '1_avg_quant': 2597.0833333333335, '2_sum_quant': 68671, '2_count_quant': 26}, ('Knuth',): {'1_sum_quant': 52557, '1_count_quant': 21, '1_avg_quant': 2502.714285714286, '2_sum_quant': 42857, '2_count_quant': 19}}.items():
    if value['1_sum_quant'] > 0.3 * value['2_sum_quant']:
        table_row = []

        for ele in key:
            table_row.append(ele)
        
        for projAttr in data['select']:
            for k, val in value.items():
                if(projAttr == k):
                    table_row.append(val)

        table.add_row(table_row)
print(table)

