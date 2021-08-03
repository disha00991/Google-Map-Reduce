from os import listdir
from os.path import isfile, join
from collections import Counter, defaultdict

def test(inputfilepath, outputfilepath):
    expected_output = getExpected(inputfilepath)
    actual_output = getActual(outputfilepath)

    if len(expected_output.keys()) == len(actual_output.keys()):
        for key in expected_output.keys():
            if (key not in actual_output.keys()) or (actual_output[key] != expected_output[key]):
                print(key)
                print("\n** URL Frequency Testcase Failed **")
                return
        print("\n** URL Frequency Testcase Passed **")
    else:
        print("\n** URL Frequency Testcase Failed **")


def getExpected(inputfilepath):
    # Open the file
    f = open(inputfilepath, 'r', encoding='utf-8')
    data = f.read()
    f.close()

    # print(Counter(data.split("\n")))
    # Return count of all URLs
    return Counter(data.split("\n"))

def getActual(outputdir):
    files = listdir(outputdir)
    urls = defaultdict(int)

    for file in files:
        if isfile(join(outputdir, file)):
            f = open(join(outputdir, file), 'r')
            data = f.read()
            f.close()
            for line in data.split("\n"):
                line = line.strip()
                if len(line)>0:
                    url, count = line.split(" ")
                    urls[url] += int(count)
    # print(urls)
    return urls