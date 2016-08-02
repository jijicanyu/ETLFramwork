import sys
import datetime
try:
    for line in sys.stdin:
        raw_data = line.strip('\n').split('\t')
        output_arr = []
        item_len = 1
        output_arr = [item_str.split(",") for item_str in raw_data]

        item_len = 1
        for item in output_arr:
            item_len = max(item_len, len(item))

        for index in range(item_len):
            line  = []
            for ele in output_arr:
                ele_idx =  (index < len(ele) and [index] or [len(ele)-1])[0]
                line.append(ele[ele_idx])
            print "\t".join(line)
except Exception, e:
    print >> sys.stderr, e, raw_data
