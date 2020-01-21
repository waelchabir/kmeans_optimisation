import sys
import shutil
import os
import logging
from random import seed, random

OUTPUT_FILE_NAME = "generated.txt"
INFO_TAG = "[Info] "

def main():
    if (len(sys.argv) != 3):
        print("\nThis function requires 2 arguments")
        print("First argument: path of the input dataset")
        print("Second argument: data duplication count")
        print("EXAMPLE: python generate_data.py <input_file> <number_of_duplication>\n")
        print("ERROR: Invalid input parameters ....\n")
        sys.exit()
    
    #Valid inputs
    input_path = sys.argv[1]
    duplication_count = int(sys.argv[2])

    for file in os.listdir('.'):
        if file == OUTPUT_FILE_NAME:
            os.remove(OUTPUT_FILE_NAME)
            print(INFO_TAG + 'Removed old generated file')
    shutil.copyfile(input_path, OUTPUT_FILE_NAME)

    for x in range(duplication_count-1):
        input_file = open(input_path, 'r')
        output_file = open(OUTPUT_FILE_NAME, 'a')
        while True:
            line = input_file.readline()
            if not line:
                break
            elements = line.split(',')
            seed(1)
            new_line = ""
            for i in range(len(elements)-1):
                elements[i] = float(elements[i])
                new_line = new_line \
                    + str(float("{0:.2f}".format(elements[i])) \
                    + float("{0:.2f}".format(random()))) \
                    + ","
            new_line = new_line + str(elements[len(elements)-1])
            output_file.write(new_line)

    print(INFO_TAG + 'Success!')

if __name__ == "__main__":
    main()