import csv
import random
import time

header = ["Time", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17",
"V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount", "Class"] 

with open("/Users/pmmahmudulhassan/Documents/SparkML/CreditCardFraudDetection/creditcard2.csv", "w") as csvfile:
	csvwriter = csv.writer(csvfile)
	# csvwriter.writerow(header)

	try:
		with open("/Users/pmmahmudulhassan/Documents/SparkML/CreditCardFraudDetection/creditcard.csv","r") as f:
			reader = csv.reader(f,delimiter = ",")
			data = list(reader)


		for x in range(0, len(data) - 1):
			chosen_row = random.choice(data)
			print(chosen_row)
			csvwriter.writerow(chosen_row)
			time.sleep(5)

	except:
		print("An error occurred while reading the file.")

	finally:
		f.close()

	csvfile.close()