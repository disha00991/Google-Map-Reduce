import re
from collections import defaultdict
from os import listdir
from os.path import isfile, join

def test(inputfilepath, outputfilepath):
    expected_output = getExpected(inputfilepath) # Retrieve wordcount from expected file
    actual_output = getActual(outputfilepath) # Retrieve wordcount from actual output file

    if len(expected_output.keys()) == len(actual_output.keys()):
        for key in expected_output.keys():
            if (key not in actual_output.keys()) or (actual_output[key] != expected_output[key]):
                print("\n** Word Count Testcase Failed! **")
                return
        print("\n** Word Count Testcase Passed! **")
    else:
        print("\n** Word Count Testcase Failed! **")


def getExpected(inputfilepath):
    # Open the file
    f = open(inputfilepath)
    data = f.read()
    f.close()

    # Process and clean data
    data = data.lower() 
    data = re.sub(r'[^\w\s]', '', data)
    words = data.split(" ")
    wordcount = defaultdict(int)

    # Iterate through words to find the word count
    for word in words:
        word = word.strip()
        if len(word) > 0:
            wordcount[word] += 1
    return wordcount


def getActual(outputdir):
    files = listdir(outputdir)
    wordcount = defaultdict(int)

    for file in files:
        if isfile(join(outputdir, file)):
            # Open output file
            f = open(join(outputdir, file), 'r')
            data = f.read()
            f.close()

            # Iterate through data to find the word count
            for line in data.split("\n"):
                line = line.strip()
                if len(line) > 0:
                    word, count = line.split(" ")
                    wordcount[word] += int(count)

    return wordcount

